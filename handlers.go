package main

import (
	"encoding/json"
	"fmt"
	"github.com/marcsello/e2-extra-input-server/distributor"
	"gitlab.com/MikeTTh/env"
	"go.uber.org/zap"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var readKey = env.StringOrPanic("READ_KEY")
var writeKey = env.StringOrPanic("WRITE_KEY")
var reqIDCounter atomic.Uint64

type coolHandler func(http.ResponseWriter, *http.Request, *zap.Logger)

func wrapHandler(handler coolHandler) http.HandlerFunc {
	var handlerName = runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		reqID := reqIDCounter.Add(1)
		logger := globalLogger.With(
			zap.String("RemoteAddr", r.RemoteAddr),
			zap.String("Path", r.URL.Path), // logging URI would expose keys...
			zap.Uint64("reqID", reqID),
			zap.String("Method", r.Method),
			zap.String("handler", handlerName),
		)
		logger.Debug("Request start")
		defer func() {
			if r := recover(); r != nil {
				logger.Error("Handler panicked (recovered)", zap.Any("reason", r))
			}
		}()

		handler(w, r, logger)
		logger.Debug("Request completed", zap.Duration("requestTime", time.Since(startTime)))
	}
}

func handleSubscription(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	var err error
	defer func() {
		if err != nil {
			logger.Error("Error while handling request", zap.Error(err))
		}
	}()

	// parse and validate key
	const keyParamName = "key"
	keyStr := r.URL.Query().Get(keyParamName)
	if keyStr != readKey {
		logger.Debug("Authorization failed", zap.Bool("fieldPresent", r.URL.Query().Has(keyParamName)))
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return // user error
	}

	// parse last id
	var lastID uint64 = 0
	const lastParamName = "last"
	if r.URL.Query().Has(lastParamName) { // last is provided
		var userErr error
		lastID, userErr = strconv.ParseUint(r.URL.Query().Get(lastParamName), 10, 64)
		if userErr != nil {
			logger.Debug("Could not parse lastID", zap.Error(userErr))
			http.Error(w, userErr.Error(), http.StatusBadRequest)
			return // user error
		}
		logger.Debug("lastID parsed", zap.Uint64("lastID", lastID))
	} else {
		logger.Debug("lastID not provided, assuming 0")
	}

	// get flusher
	flusher, ok := w.(http.Flusher)
	if !ok {
		err = fmt.Errorf("could not acquire ResponseWriter flusher")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// subscribe
	newDataChan, unsub := globalDistributor.Subscribe(lastID)
	delivered := false
	defer func() {
		unsub(delivered && err == nil)
	}()

	// prepare timers
	keepaliveTicker := time.NewTicker(time.Second * 50)
	defer keepaliveTicker.Stop()

	timeoutTimer := time.NewTimer(time.Minute * 10)
	defer timeoutTimer.Stop()

	// start the show
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	logger.Debug("Wait loop start")
	for {
		select {
		case data := <-newDataChan:
			logger.Debug("Data event received", zap.Uint64("data.ID", data.ID))
			var dataJSON []byte
			dataJSON, err = json.Marshal(data)
			if err != nil {
				return // the deferred func will log the error, nothing to do
			}
			_, err = w.Write(dataJSON)
			if err != nil {
				return // the deferred func will log the error, nothing to do
			}
			delivered = true
			return // we are done \o/
		case <-keepaliveTicker.C:
			logger.Debug("Keepalive ticker tick")
			_, err = w.Write([]byte(" "))
			if err != nil {
				return // the deferred func will log the error, nothing to do
			}
			flusher.Flush()
		case <-timeoutTimer.C:
			logger.Debug("Connection timed out by the server")
			// we just write the last known state in this case
			data := globalDistributor.GetLastData() // returns empty struct if not yet set
			var dataJSON []byte
			dataJSON, err = json.Marshal(data)
			if err != nil {
				return // the deferred func will log the error, nothing to do
			}
			_, err = w.Write(dataJSON)
			if err != nil {
				return // the deferred func will log the error, nothing to do
			}
			return // nothing to do
		case <-r.Context().Done():
			logger.Debug("Client disconnected", zap.Error(r.Context().Err()))
			return //nothing to do
		}
	}
}

func handlePublish(w http.ResponseWriter, r *http.Request, logger *zap.Logger) {
	var err error
	defer func() {
		if err != nil {
			logger.Error("Error while handling request", zap.Error(err))
		}
	}()

	// check key
	authHeaderStr := r.Header.Get("Authorization")
	if authHeaderStr == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		logger.Debug("Authorization failed", zap.Bool("fieldPresent", false))
		return
	}
	authHeaderParts := strings.SplitN(authHeaderStr, " ", 3)
	if len(authHeaderParts) != 2 || authHeaderParts[0] != "Key" || authHeaderParts[1] != writeKey {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		logger.Debug("Authorization failed", zap.Bool("fieldPresent", true))
		return
	}

	// parse body
	var data distributor.Data
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	globalDistributor.Distribute(data)

	var dataJSON []byte
	dataJSON, err = json.Marshal(data)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(dataJSON)
	if err != nil { // not really needed, but would forget otherwise
		return
	}
}
