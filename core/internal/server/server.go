package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/ssr-adapter/go/core/adapter"
)

type Server struct {
	adapter    *adapter.Adapter
	port       int
	HttpServer *http.Server
}

func NewServer() *Server {
	port, _ := strconv.Atoi(os.Getenv("PORT"))

	a, err := adapter.NewAdapter(&adapter.AdapterConfig{Worker: 1})
	if err != nil {
		panic(err)
	}

	server := &Server{
		adapter: a,
		port:    port,
	}

	// Declare Server config
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", server.port),
		Handler:      server.RegisterRoutes(),
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	server.HttpServer = httpServer

	return server
}

func (s *Server) Shutdown(ctx context.Context) error {
	err := s.HttpServer.Shutdown(ctx)
	if err != nil {
		return err
	}
	err = s.adapter.Stop()
	if err != nil {
		return err
	}
	return nil
}
