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

	globalLogger.Info("Starting webserver")
	err = http.ListenAndServe(":8080", r)
	if err != nil {
		panic(err)
	}
}
