package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/philips-forks/zip_streamer/zip_streamer"
)

func main() {
	zipServer, err := zip_streamer.NewServer()
	if err != nil {
		log.Print("No S3 service found, shutting down...")
		return
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "4008"
	}

	httpServer := &http.Server{
		Addr:        ":" + port,
		Handler:     zipServer,
		ReadTimeout: 10 * time.Second,
	}

	log.Printf("Server starting on port %s", port)
	go func() {
		httpServer.ListenAndServe()
	}()

	// Gracefully shutdown when SIGTERM is received
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Print("Received SIGTERM, shutting down...")
	httpServer.Shutdown(context.Background())
}
