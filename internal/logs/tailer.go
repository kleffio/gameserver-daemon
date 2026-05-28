package logs

import (
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/kleffio/kleff-daemon/internal/application/ports"
)

const (
	batchSize     = 100
	flushInterval = 5 * time.Second
)

// Tailer manages one log-tailing goroutine per running workload.
// It mirrors the Scraper pattern: call Run to start, cancel the context to stop.
type Tailer struct {
	runtime ports.RuntimeAdapter
	repo    ports.ServerRepository
	shipper ports.LogShipper
	logger  ports.Logger

	mu      sync.Mutex
	running map[string]context.CancelFunc // workloadID → cancel
}

func NewTailer(
	runtime ports.RuntimeAdapter,
	repo ports.ServerRepository,
	shipper ports.LogShipper,
	logger ports.Logger,
) *Tailer {
	return &Tailer{
		runtime: runtime,
		repo:    repo,
		shipper: shipper,
		logger:  logger,
		running: make(map[string]context.CancelFunc),
	}
}

// Run blocks until ctx is cancelled, reconciling the set of active tailers
// every 10 seconds to match the set of running workloads.
func (t *Tailer) Run(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	// Reconcile immediately on start.
	t.reconcile(ctx)
	for {
		select {
		case <-ctx.Done():
			t.stopAll()
			return
		case <-ticker.C:
			t.reconcile(ctx)
		}
	}
}

func (t *Tailer) reconcile(ctx context.Context) {
	servers, err := t.repo.ListAll(ctx)
	if err != nil {
		t.logger.Warn("log tailer: failed to list servers", "error", err)
		return
	}

	active := make(map[string]struct{}, len(servers))
	for _, srv := range servers {
		if srv.Status != "Running" && srv.Status != "running" {
			continue
		}
		active[srv.ID] = struct{}{}

		t.mu.Lock()
		_, already := t.running[srv.ID]
		t.mu.Unlock()

		if !already {
			wctx, cancel := context.WithCancel(ctx)
			t.mu.Lock()
			t.running[srv.ID] = cancel
			t.mu.Unlock()
			go t.tail(wctx, srv.ID, srv.ProjectID, srv.EnvironmentID)
		}
	}

	// Stop tailers for workloads that are no longer running.
	t.mu.Lock()
	for id, cancel := range t.running {
		if _, ok := active[id]; !ok {
			cancel()
			delete(t.running, id)
		}
	}
	t.mu.Unlock()
}

func (t *Tailer) stopAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, cancel := range t.running {
		cancel()
		delete(t.running, id)
	}
}

// tail streams logs for one workload until ctx is cancelled or the container exits.
func (t *Tailer) tail(ctx context.Context, workloadID, projectID, environmentID string) {
	defer func() {
		t.mu.Lock()
		delete(t.running, workloadID)
		t.mu.Unlock()
	}()

	scopeID := projectID
	if scopeID == "" {
		scopeID = environmentID
	}
	rc, err := t.runtime.Logs(ctx, scopeID, workloadID, true)
	if err != nil {
		t.logger.Warn("log tailer: failed to open log stream", "workload_id", workloadID, "error", err)
		return
	}
	defer rc.Close()

	// Docker multiplexes stdout/stderr in an 8-byte framed format.
	// stdcopy.StdCopy demuxes them into separate writers.
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	go func() {
		_, _ = stdcopy.StdCopy(stdoutW, stderrW, rc)
		stdoutW.Close()
		stderrW.Close()
	}()

	batch := make([]ports.LogEntry, 0, batchSize)
	flush := time.NewTicker(flushInterval)
	defer flush.Stop()

	lines := make(chan ports.LogEntry, 256)

	var scanWg sync.WaitGroup
	scanWg.Add(2)
	go func() { defer scanWg.Done(); scanStream(ctx, stdoutR, "stdout", lines) }()
	go func() { defer scanWg.Done(); scanStream(ctx, stderrR, "stderr", lines) }()
	go func() { scanWg.Wait(); close(lines) }()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				_ = t.shipper.ShipLogs(context.Background(), workloadID, projectID, environmentID, batch)
			}
			return
		case entry, ok := <-lines:
			if !ok {
				if len(batch) > 0 {
					_ = t.shipper.ShipLogs(context.Background(), workloadID, projectID, environmentID, batch)
				}
				return
			}
			batch = append(batch, entry)
			if len(batch) >= batchSize {
				if err := t.shipper.ShipLogs(ctx, workloadID, projectID, environmentID, batch); err != nil {
					t.logger.Warn("log tailer: ship failed", "workload_id", workloadID, "error", err)
				}
				batch = batch[:0]
			}
		case <-flush.C:
			if len(batch) > 0 {
				if err := t.shipper.ShipLogs(ctx, workloadID, projectID, environmentID, batch); err != nil {
					t.logger.Warn("log tailer: ship failed", "workload_id", workloadID, "error", err)
				}
				batch = batch[:0]
			}
		}
	}
}

func scanStream(ctx context.Context, r io.Reader, stream string, out chan<- ports.LogEntry) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		case out <- ports.LogEntry{Ts: time.Now().UTC(), Stream: stream, Line: scanner.Text()}:
		}
	}
}
