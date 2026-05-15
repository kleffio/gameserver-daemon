package ports

import (
	"context"
	"time"
)

// LogEntry is one line of container output.
type LogEntry struct {
	Ts     time.Time
	Stream string // "stdout" or "stderr"
	Line   string
}

// LogShipper ships batches of log lines to the platform.
type LogShipper interface {
	ShipLogs(ctx context.Context, workloadID, projectID, environmentID string, lines []LogEntry) error
}

// NoopLogShipper discards all log lines. Used when log shipping is disabled.
type NoopLogShipper struct{}

func (NoopLogShipper) ShipLogs(_ context.Context, _, _, _ string, _ []LogEntry) error { return nil }
