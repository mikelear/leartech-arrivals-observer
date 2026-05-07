// Package main is the leartech-arrivals-observer entrypoint.
//
// Watches K8s ReplicaSet "Added" events in jx-staging (per-cluster) and
// creates Arrival CRs (qa.leartech.com/v1alpha1) for each new service
// version it observes. Phase 2.7.1 scope: just the watch + CR creation.
// Test dispatch (Phase 2.7.2), traffic forensics (2.7.3), Slack alerts
// (2.7.4) are follow-on sessions.
//
// Health + metrics on the existing port (8080) for K8s probes + Prometheus.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/mikelear/leartech-arrivals-observer/internal/config"
	"github.com/mikelear/leartech-arrivals-observer/internal/controller"
	"github.com/mikelear/leartech-arrivals-observer/internal/handlers"
	"github.com/mikelear/leartech-arrivals-observer/internal/middleware"
	"github.com/mikelear/leartech-arrivals-observer/internal/tracing"
	"github.com/mikelear/leartech-arrivals-observer/internal/watcher"
)

// version is injected at build time via -ldflags "-X main.version=<version>".
var version = "dev"

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if err := run(); err != nil {
		log.Fatal().Err(err).Msg("observer failed")
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info().
		Str("version", version).
		Str("clusterID", cfg.ClusterID).
		Str("watchNamespace", cfg.WatchNamespace).
		Str("port", cfg.Port).
		Msg("starting leartech-arrivals-observer")

	shutdownTracer, err := tracing.Init(ctx, "leartech-arrivals-observer", version, cfg.ClusterID)
	if err != nil {
		return fmt.Errorf("init tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			log.Warn().Err(err).Msg("tracer shutdown failed")
		}
	}()

	services, err := cfg.LoadServices()
	if err != nil {
		return fmt.Errorf("load services map: %w", err)
	}
	log.Info().Int("services", len(services)).Msg("services map loaded")

	// Start the ReplicaSet informer + Arrival CR creator in a goroutine.
	// On every Added event matching the filter (chart-managed Deployments
	// in cfg.WatchNamespace), upserts an Arrival CR keyed by
	// <service>-<version>-<namespace>. Per-service stagingUrl + testPacks
	// come from the services map (chart values).
	w, err := watcher.New(ctx, watcher.Config{
		Namespace:      cfg.WatchNamespace,
		ClusterID:      cfg.ClusterID,
		KubeConfigPath: cfg.KubeConfigPath, // empty = in-cluster
		Services:       services,
	})
	if err != nil {
		return fmt.Errorf("init watcher: %w", err)
	}
	go w.Run(ctx)

	// Start the Arrival lifecycle controller. Drives Pending → Skipped /
	// Testing / Passed / Failed / Timeout. Phase 2.7.2 first cut: stub
	// dispatch (no real K8s Job); 2.7.2b will swap in real Job dispatch
	// + result-store polling without changing the state machine.
	ctrl, err := controller.New(ctx, controller.Config{
		Namespace:      cfg.WatchNamespace,
		KubeConfigPath: cfg.KubeConfigPath,
		PollInterval:   cfg.DispatchPollInterval(),
		Timeout:        cfg.DispatchTimeout(),
	})
	if err != nil {
		return fmt.Errorf("init controller: %w", err)
	}
	go ctrl.Run(ctx)

	// Health + metrics HTTP server (K8s probes, Prometheus scrape).
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(otelgin.Middleware("leartech-arrivals-observer"))
	router.Use(middleware.RequestLogger())

	healthHandler := handlers.NewHealthHandler(nil, version)
	healthHandler.RegisterRoutes(router)
	handlers.RegisterMetrics(router)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("server listen failed")
		}
	}()

	log.Info().Str("port", cfg.Port).Msg("observer started")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server forced shutdown: %w", err)
	}

	log.Info().Msg("observer stopped")
	return nil
}
