package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kleffio/kleff-daemon/internal/adapters/in/fileapi"
	"github.com/kleffio/kleff-daemon/internal/adapters/out/db"
	"github.com/kleffio/kleff-daemon/internal/adapters/out/observability/logging"
	platformadapter "github.com/kleffio/kleff-daemon/internal/adapters/out/platform"
	queueadapter "github.com/kleffio/kleff-daemon/internal/adapters/out/queue"
	memrepo "github.com/kleffio/kleff-daemon/internal/adapters/out/repository/memory"
	dockeradapter "github.com/kleffio/kleff-daemon/internal/adapters/out/runtime/docker"
	k8sadapter "github.com/kleffio/kleff-daemon/internal/adapters/out/runtime/kubernetes"
	"github.com/kleffio/kleff-daemon/internal/app/config"
	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/internal/logs"
	"github.com/kleffio/kleff-daemon/internal/metrics"
	"github.com/kleffio/kleff-daemon/internal/workers"
	"github.com/kleffio/kleff-daemon/internal/workers/jobs"
	"k8s.io/client-go/rest"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to start daemon: %v", err)
	}

	baseLogger := logging.NewSlogAdapter()
	daemonLog := baseLogger.With(ports.LogKeyNodeID, cfg.NodeID)

	// --- Runtime adapter: auto-detect Docker vs Kubernetes ---
	runtime, err := detectRuntime(cfg, daemonLog)
	if err != nil {
		daemonLog.Error("Failed to initialize runtime adapter", err)
		os.Exit(1)
	}

	// --- Database ---
	sqliteDB, err := db.InitDB(cfg.DatabasePath)
	if err != nil {
		daemonLog.Error("Failed to initialize database", err, "path", cfg.DatabasePath)
		os.Exit(1)
	}
	defer sqliteDB.Close()
	daemonLog.Info("Database initialized", "path", cfg.DatabasePath)

	// --- Queue ---
	var q ports.Queue
	switch cfg.QueueBackend {
	case config.QueueBackendRedis:
		rq, err := queueadapter.NewRedisQueue(cfg.RedisURL, cfg.RedisPassword, cfg.RedisTLS)
		if err != nil {
			daemonLog.Error("Failed to initialize Redis queue", err)
			os.Exit(1)
		}
		q = rq
		daemonLog.Info("Queue backend: Redis", "url", cfg.RedisURL)
	default:
		q = queueadapter.NewMemoryQueue()
		daemonLog.Info("Queue backend: in-memory")
	}

	// --- Repository ---
	repo := memrepo.NewServerRepository()

	// Re-adopt containers from a previous daemon run so the tailer and scraper
	// resume without waiting for the next provision job.
	if existing, discErr := runtime.Discover(context.Background()); discErr != nil {
		daemonLog.Warn("Failed to discover existing workloads on startup", "error", discErr)
	} else {
		for _, rec := range existing {
			if saveErr := repo.Save(context.Background(), rec); saveErr != nil {
				daemonLog.Warn("Failed to restore workload record", "workload_id", rec.ID, "error", saveErr)
			}
		}
		if len(existing) > 0 {
			daemonLog.Info("Restored workload records from runtime", "count", len(existing))
		}
	}

	// --- File API server ---
	fileServer := fileapi.NewServer(cfg.StoragePath, cfg.SharedSecret, cfg.FileAPIPort, daemonLog)
	go func() {
		if err := fileServer.Start(); err != nil {
			daemonLog.Warn("File API server stopped", "error", err)
		}
	}()


	// --- Platform registration + status reporting ---
	platformClient := platformadapter.NewClient(cfg.PlatformURL, cfg.SharedSecret, cfg.NodeID, cfg.FileAPIURL, daemonLog)
	if err := platformClient.RegisterNode(context.Background()); err != nil {
		daemonLog.Error("Failed to register node with platform", err)
		os.Exit(1)
	}

	// --- Dispatcher + workers ---
	dispatcher := workers.NewDispatcher(q, 4)
	dispatcher.Register(jobs.JobTypeServerProvision, workers.NewProvisionWorker(runtime, repo, daemonLog, platformClient).Handle)
	dispatcher.Register(jobs.JobTypeServerStart, workers.NewStartWorker(runtime, repo, daemonLog, platformClient).Handle)
	dispatcher.Register(jobs.JobTypeServerStop, workers.NewStopWorker(runtime, repo, daemonLog, platformClient).Handle)
	dispatcher.Register(jobs.JobTypeServerDelete, workers.NewDeleteWorker(runtime, repo, daemonLog, platformClient).Handle)
	dispatcher.Register(jobs.JobTypeServerRestart, workers.NewRestartWorker(runtime, repo, daemonLog, platformClient).Handle)

	modWorker := workers.NewModWorker(runtime, daemonLog)
	dispatcher.Register(jobs.JobTypeModInstall, modWorker.HandleInstall)
	dispatcher.Register(jobs.JobTypeModUninstall, modWorker.HandleUninstall)

	daemonLog.Info("Daemon started", "node_id", cfg.NodeID, "grpc_port", cfg.GRPCPort)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	scrapeInterval := time.Duration(cfg.MetricsScrapeInterval) * time.Second
	scraper := metrics.NewScraper(runtime, repo, platformClient, scrapeInterval, cfg.NodeID, daemonLog)
	go scraper.Run(ctx)

	tailer := logs.NewTailer(runtime, repo, platformClient, daemonLog)
	go tailer.Run(ctx)

	dispatcher.Run(ctx)

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = fileServer.Shutdown(shutCtx)

	daemonLog.Info("Daemon shutdown complete")
}

func detectRuntime(cfg *config.Config, logger ports.Logger) (ports.RuntimeAdapter, error) {
	ctx := context.Background()

	// Explicit kubeconfig → always Kubernetes, fail hard if unreachable.
	if cfg.Kubeconfig != "" {
		adapter, err := k8sadapter.New(cfg.Kubeconfig, cfg.KubeNamespace, cfg.NodeID)
		if err != nil {
			return nil, fmt.Errorf("kubernetes runtime unavailable: %w", err)
		}
		logger.Info("Runtime: Kubernetes", "kubeconfig", cfg.Kubeconfig)
		return adapter, nil
	}

	// In-cluster environment → Kubernetes, fail hard if unreachable.
	if _, err := rest.InClusterConfig(); err == nil {
		adapter, err := k8sadapter.New("", cfg.KubeNamespace, cfg.NodeID)
		if err != nil {
			return nil, fmt.Errorf("kubernetes in-cluster runtime unavailable: %w", err)
		}
		logger.Info("Runtime: Kubernetes (in-cluster)")
		return adapter, nil
	}

	// No Kubernetes — check if Docker is actually reachable before using it.
	dockerAdapter, err := dockeradapter.New(cfg.NodeID, cfg.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("no runtime available: kubernetes not detected, docker client failed: %w", err)
	}
	if err := dockerAdapter.Ping(ctx); err != nil {
		return nil, fmt.Errorf("no runtime available: kubernetes not detected, docker unreachable: %w", err)
	}
	logger.Info("Runtime: Docker")
	return dockerAdapter, nil
}
