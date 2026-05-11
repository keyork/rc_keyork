// Command server is the single binary entry point for rc_keyork.
// The ROLE env var selects which subsystems start:
//
//	ROLE=api     — HTTP API only (submit, query, retry endpoints)
//	ROLE=worker  — delivery worker + zombie recovery only
//	ROLE=all     — both (default, suitable for single-node runs)
//
// Set MOCK=true to use in-memory fakes for RabbitMQ and PostgreSQL so the
// service can run without any external infrastructure.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/keyork/rc_keyork/internal/api"
	"github.com/keyork/rc_keyork/internal/circuitbreaker"
	"github.com/keyork/rc_keyork/internal/config"
	"github.com/keyork/rc_keyork/internal/db"
	dbmock "github.com/keyork/rc_keyork/internal/db/mock"
	"github.com/keyork/rc_keyork/internal/logger"
	"github.com/keyork/rc_keyork/internal/mq"
	mqmock "github.com/keyork/rc_keyork/internal/mq/mock"
	"github.com/keyork/rc_keyork/internal/worker"
)

func main() {
	cfg := config.Load()

	// Initialise structured logging before any other output.
	logger.Init(cfg.Log.Level, cfg.Log.Format)

	slog.Info("starting rc_keyork",
		"role", cfg.Role,
		"mock", cfg.Mock,
		"addr", cfg.HTTP.Addr,
		"log_level", cfg.Log.Level,
		"log_format", cfg.Log.Format,
	)

	// --- wire dependencies ---

	var (
		store db.Store
		pub   mq.Publisher
		con   mq.Consumer
	)

	if cfg.Mock {
		mockStore := dbmock.New()
		mockQueue := mqmock.New()
		store = mockStore
		pub = mockQueue
		con = mockQueue
		slog.Info("using in-memory mocks for DB and MQ", "component", "main")
	} else {
		// Real adapters (RabbitMQ + PostgreSQL) are not yet implemented.
		slog.Error("real adapters not yet implemented; set MOCK=true", "component", "main")
		os.Exit(1)
	}

	cb := circuitbreaker.NewManager(circuitbreaker.Config{
		WindowDur:    cfg.CB.WindowDur,
		MinRequests:  cfg.CB.MinRequests,
		FailureRatio: cfg.CB.FailureRatio,
		OpenDur:      cfg.CB.OpenDur,
	})

	workerCfg := worker.Config{
		Concurrency:     cfg.Worker.Concurrency,
		HTTPTimeout:     cfg.Worker.HTTPTimeout,
		CB:              cb,
		CallbackDelays:  cfg.Worker.CallbackDelays,
		ZombieInterval:  cfg.Worker.ZombieInterval,
		ZombieThreshold: cfg.Worker.ZombieThreshold,
	}

	// --- signal handling & root context ---

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("shutdown signal received", "component", "main", "signal", sig.String())
		cancel()
	}()

	// --- API server ---

	var httpServer *http.Server
	if cfg.Role == "api" || cfg.Role == "all" {
		h := api.NewHandler(store, pub, api.HandlerConfig{
			MaxRetries:      cfg.Notification.MaxRetries,
			DefaultPageSize: cfg.Notification.DefaultPageSize,
		})
		httpServer = &http.Server{
			Addr:    cfg.HTTP.Addr,
			Handler: api.NewServeMux(h),
		}
		go func() {
			slog.Info("API server listening", "component", "main", "addr", cfg.HTTP.Addr)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("HTTP server crashed", "component", "main", "error", err)
				os.Exit(1)
			}
		}()
	}

	// --- Worker pool + zombie recovery ---

	if cfg.Role == "worker" || cfg.Role == "all" {
		pool := worker.NewPool(store, con, pub, workerCfg)
		go func() {
			if err := pool.Run(ctx); err != nil {
				slog.Error("worker pool stopped with error", "component", "main", "error", err)
			}
		}()

		zombie := worker.NewZombieRecovery(store, pub, workerCfg)
		go zombie.Run(ctx)
	}

	// --- graceful shutdown ---

	<-ctx.Done()
	slog.Info("initiating graceful shutdown", "component", "main", "grace_period", cfg.Shutdown.GracePeriod)

	if httpServer != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), cfg.Shutdown.GracePeriod)
		defer shutCancel()
		if err := httpServer.Shutdown(shutCtx); err != nil {
			slog.Error("HTTP server shutdown error", "component", "main", "error", err)
		}
	}

	if err := pub.Close(); err != nil {
		slog.Error("publisher close error", "component", "main", "error", err)
	}
	if err := con.Close(); err != nil {
		slog.Error("consumer close error", "component", "main", "error", err)
	}

	slog.Info("shutdown complete", "component", "main")
}
