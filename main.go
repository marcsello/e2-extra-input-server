package main

import (
	"github.com/gorilla/mux"
	"github.com/marcsello/e2-extra-input-server/distributor"
	"gitlab.com/MikeTTh/env"
	"go.uber.org/zap"
	"net/http"
)

var globalLogger *zap.Logger
var globalDistributor *distributor.Distributor

func main() {
	var err error
	debug := env.Bool("DEBUG", false)

	if debug {
		globalLogger, err = zap.NewDevelopment()
	} else {
		globalLogger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}

	defer globalLogger.Sync()

	globalDistributor = distributor.NewDistributor(globalLogger.With(zap.String("src", "distributor"), zap.String("distributorInstance", "global")))

	r := mux.NewRouter()
	r.Methods("GET").Path("/sub").HandlerFunc(wrapHandler(handleSubscription)) // middlewares are just pain to use...
	r.Methods("PUT").Path("/pub").HandlerFunc(wrapHandler(handlePublish))

	certFile := env.String("TLS_CERT", "")
	keyFile := env.String("TLS_KEY", "")
	useTLS := certFile != "" && keyFile != ""
	bindAddr := env.String("BIND_ADDRESS", ":8080")

	globalLogger.Info("Starting webserver", zap.Bool("useTLS", useTLS), zap.String("bindAddr", bindAddr))
	if useTLS {
		err = http.ListenAndServeTLS(bindAddr, certFile, keyFile, r)
	} else {
		err = http.ListenAndServe(bindAddr, r)
	}
	if err != nil {
		panic(err)
	}
}
