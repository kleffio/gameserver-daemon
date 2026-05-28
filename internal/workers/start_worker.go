package workers

import (
	"context"
	"fmt"
	"strings"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/internal/workers/jobs"
)

type StartWorker struct {
	runtime    ports.RuntimeAdapter
	repository ports.ServerRepository
	logger     ports.Logger
	reporter   ports.WorkloadStatusReporter
}

func NewStartWorker(runtime ports.RuntimeAdapter, repository ports.ServerRepository, logger ports.Logger, reporters ...ports.WorkloadStatusReporter) *StartWorker {
	var reporter ports.WorkloadStatusReporter = ports.NoopWorkloadStatusReporter{}
	if len(reporters) > 0 && reporters[0] != nil {
		reporter = reporters[0]
	}
	return &StartWorker{runtime: runtime, repository: repository, logger: logger, reporter: reporter}
}

func (w *StartWorker) Handle(ctx context.Context, job *jobs.Job) error {
	log := w.logger.With(ports.LogKeyJobID, job.JobID, ports.LogKeyWorkerType, "server.start")

	var spec ports.WorkloadSpec
	if err := job.UnmarshalPayload(&spec); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	if spec.ProjectID == "" {
		if spec.EnvironmentID != "" {
			spec.ProjectID = spec.EnvironmentID
		} else if spec.NamespaceID != "" {
			spec.ProjectID = spec.NamespaceID
		}
	}
	if spec.ProjectID == "" {
		return fmt.Errorf("invalid payload: project_id or namespace_id is required")
	}

	report := func(status, runtimeRef, endpoint, errMsg string) {
		if err := w.reporter.ReportStatus(ctx, ports.WorkloadStatusUpdate{
			WorkloadID:   spec.ServerID,
			ProjectID:    spec.ProjectID,
			Status:       status,
			RuntimeRef:   runtimeRef,
			Endpoint:     endpoint,
			ErrorMessage: errMsg,
		}); err != nil {
			log.Warn("Failed to report workload status", "workload_id", spec.ServerID, "error", err)
		}
	}

	log.Info("Starting server", ports.LogKeyServerID, spec.ServerID)

	server, err := w.runtime.Start(ctx, spec)
	if err != nil {
		log.Error("Failed to start server", err)
		report("failed", "", "", err.Error())
		return fmt.Errorf("start failed: %w", err)
	}

	if err := w.repository.UpdateStatus(ctx, spec.ServerID, server.State); err != nil {
		log.Warn("Failed to update server status after start", "server_id", spec.ServerID)
	}

	endpoint, epErr := w.runtime.Endpoint(ctx, spec.ProjectID, spec.ServerID)
	if epErr != nil {
		log.Warn("Failed to resolve endpoint after start", "workload_id", spec.ServerID, "error", epErr)
	}
	report(strings.ToLower(server.State), server.RuntimeRef, endpoint, "")

	log.Info("Server started successfully", ports.LogKeyServerID, spec.ServerID)
	return nil
}
