package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
)

type ioSnapshot struct {
	rxMB    float64
	txMB    float64
	diskRMB float64
	diskWMB float64
}

// Scraper periodically collects per-container metrics and reports them to the platform.
type Scraper struct {
	runtime  ports.RuntimeAdapter
	repo     ports.ServerRepository
	reporter ports.WorkloadStatusReporter
	interval time.Duration
	nodeID   string
	logger   ports.Logger

	mu      sync.Mutex
	prevIO  map[string]ioSnapshot // workloadID → last cumulative network+disk totals
}

func NewScraper(
	runtime ports.RuntimeAdapter,
	repo ports.ServerRepository,
	reporter ports.WorkloadStatusReporter,
	interval time.Duration,
	nodeID string,
	logger ports.Logger,
) *Scraper {
	return &Scraper{
		runtime:  runtime,
		repo:     repo,
		reporter: reporter,
		interval: interval,
		nodeID:   nodeID,
		logger:   logger,
		prevIO: make(map[string]ioSnapshot),
	}
}

// Run blocks until ctx is cancelled, scraping metrics on each tick.
func (s *Scraper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scrape(ctx)
		}
	}
}

func (s *Scraper) scrape(ctx context.Context) {
	servers, err := s.repo.ListAll(ctx)
	if err != nil {
		s.logger.Warn("metrics scraper: failed to list servers", "error", err)
		return
	}

	activeIDs := make(map[string]struct{}, len(servers))
	for _, srv := range servers {
		if srv.Status != "Running" && srv.Status != "running" {
			continue
		}
		activeIDs[srv.ID] = struct{}{}

		scopeID := srv.ProjectID
		if scopeID == "" {
			scopeID = srv.EnvironmentID
		}
		health, err := s.runtime.Status(ctx, scopeID, srv.ID)
		if err != nil {
			s.logger.Warn("metrics scraper: failed to get status", "workload_id", srv.ID, "error", err)
			continue
		}

		// Convert cumulative network+disk totals to per-interval deltas.
		s.mu.Lock()
		prev := s.prevIO[srv.ID]
		rxDelta := health.NetworkRxMB - prev.rxMB
		txDelta := health.NetworkTxMB - prev.txMB
		diskRDelta := health.DiskReadMB - prev.diskRMB
		diskWDelta := health.DiskWriteMB - prev.diskWMB
		if rxDelta < 0 {
			rxDelta = health.NetworkRxMB // container restarted
		}
		if txDelta < 0 {
			txDelta = health.NetworkTxMB
		}
		if diskRDelta < 0 {
			diskRDelta = health.DiskReadMB
		}
		if diskWDelta < 0 {
			diskWDelta = health.DiskWriteMB
		}
		s.prevIO[srv.ID] = ioSnapshot{rxMB: health.NetworkRxMB, txMB: health.NetworkTxMB, diskRMB: health.DiskReadMB, diskWMB: health.DiskWriteMB}
		s.mu.Unlock()

		update := ports.WorkloadStatusUpdate{
			WorkloadID:    srv.ID,
			ProjectID:     srv.ProjectID,
			Status:        mapDockerState(health.State),
			RuntimeRef:    srv.RuntimeRef,
			NodeID:        s.nodeID,
			CPUMillicores: health.CPUMillicores,
			MemoryMB:      health.MemoryMB,
			NetworkRxMB:   rxDelta,
			NetworkTxMB:   txDelta,
			DiskReadMB:    diskRDelta,
			DiskWriteMB:   diskWDelta,
		}
		if err := s.reporter.ReportStatus(ctx, update); err != nil {
			s.logger.Warn("metrics scraper: failed to report status", "workload_id", srv.ID, "error", err)
		}
	}

	// Clean up snapshots for workloads that are no longer active.
	s.mu.Lock()
	for id := range s.prevIO {
		if _, ok := activeIDs[id]; !ok {
			delete(s.prevIO, id)
		}
	}
	s.mu.Unlock()
}

// mapDockerState converts Docker container states to platform workload states.
func mapDockerState(dockerState string) string {
	switch dockerState {
	case "running":
		return "running"
	case "exited", "dead":
		return "stopped"
	case "created", "paused", "restarting":
		return "pending"
	case "removing":
		return "deleted"
	default:
		return "failed"
	}
}
