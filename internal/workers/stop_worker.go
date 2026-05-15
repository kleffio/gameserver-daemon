package workers

import (
	"context"
	"fmt"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/internal/workers/jobs"
)

type StopWorker struct {
	runtime    ports.RuntimeAdapter
	repository ports.ServerRepository
	logger     ports.Logger
	reporter   ports.WorkloadStatusReporter
}

func NewStopWorker(runtime ports.RuntimeAdapter, repository ports.ServerRepository, logger ports.Logger, reporters ...ports.WorkloadStatusReporter) *StopWorker {
	var reporter ports.WorkloadStatusReporter = ports.NoopWorkloadStatusReporter{}
	if len(reporters) > 0 && reporters[0] != nil {
		reporter = reporters[0]
	}
	return &StopWorker{runtime: runtime, repository: repository, logger: logger, reporter: reporter}
}

func (w *StopWorker) Handle(ctx context.Context, job *jobs.Job) error {
	log := w.logger.With(ports.LogKeyJobID, job.JobID, ports.LogKeyWorkerType, "server.stop")

	var spec ports.WorkloadSpec
	if err := job.UnmarshalPayload(&spec); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	log.Info("Stopping server", ports.LogKeyServerID, spec.ServerID)

	if spec.ProjectID == "" {
		if spec.EnvironmentID != "" {
			spec.ProjectID = spec.EnvironmentID
		}
	}
	if spec.ProjectID == "" {
		return fmt.Errorf("invalid payload: project_id is required")
	}

	report := func(status, errMsg string) {
		if err := w.reporter.ReportStatus(ctx, ports.WorkloadStatusUpdate{
			WorkloadID:   spec.ServerID,
			ProjectID:    spec.ProjectID,
			Status:       status,
			ErrorMessage: errMsg,
		}); err != nil {
			log.Warn("Failed to report workload status", "workload_id", spec.ServerID, "error", err)
		}
	}

	if err := w.runtime.Stop(ctx, spec.ProjectID, spec.ServerID); err != nil {
		log.Error("Failed to stop server", err)
		report("failed", err.Error())
		return fmt.Errorf("stop failed: %w", err)
	}

	if err := w.repository.UpdateStatus(ctx, spec.ServerID, "stopped"); err != nil {
		log.Warn("Failed to update server status after stop", "server_id", spec.ServerID)
	}
	report("stopped", "")

	log.Info("Server stopped successfully", ports.LogKeyServerID, spec.ServerID)
	return nil
}
