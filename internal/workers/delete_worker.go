package workers

import (
	"context"
	"fmt"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/internal/workers/jobs"
)

type DeleteWorker struct {
	runtime    ports.RuntimeAdapter
	repository ports.ServerRepository
	logger     ports.Logger
	reporter   ports.WorkloadStatusReporter
}

func NewDeleteWorker(runtime ports.RuntimeAdapter, repository ports.ServerRepository, logger ports.Logger, reporters ...ports.WorkloadStatusReporter) *DeleteWorker {
	var reporter ports.WorkloadStatusReporter = ports.NoopWorkloadStatusReporter{}
	if len(reporters) > 0 && reporters[0] != nil {
		reporter = reporters[0]
	}
	return &DeleteWorker{runtime: runtime, repository: repository, logger: logger, reporter: reporter}
}

func (w *DeleteWorker) Handle(ctx context.Context, job *jobs.Job) error {
	log := w.logger.With(ports.LogKeyJobID, job.JobID, ports.LogKeyWorkerType, "server.delete")

	var spec ports.WorkloadSpec
	if err := job.UnmarshalPayload(&spec); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	log.Info("Deleting server", ports.LogKeyServerID, spec.ServerID)

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

	if err := w.runtime.Remove(ctx, spec.ProjectID, spec.ServerID); err != nil {
		log.Error("Failed to delete server", err)
		report("failed", err.Error())
		return fmt.Errorf("delete failed: %w", err)
	}

	if err := w.repository.UpdateStatus(ctx, spec.ServerID, "deleted"); err != nil {
		log.Warn("Failed to update server status after delete", "server_id", spec.ServerID)
	}
	report("deleted", "")

	log.Info("Server deleted successfully", ports.LogKeyServerID, spec.ServerID)
	return nil
}
