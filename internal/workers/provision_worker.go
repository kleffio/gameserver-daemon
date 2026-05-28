package workers

import (
	"context"
	"fmt"
	"strings"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/internal/workers/jobs"
)

type ProvisionWorker struct {
	runtime    ports.RuntimeAdapter
	repository ports.ServerRepository
	logger     ports.Logger
	reporter   ports.WorkloadStatusReporter
}

func NewProvisionWorker(runtime ports.RuntimeAdapter, repository ports.ServerRepository, logger ports.Logger, reporters ...ports.WorkloadStatusReporter) *ProvisionWorker {
	var reporter ports.WorkloadStatusReporter = ports.NoopWorkloadStatusReporter{}
	if len(reporters) > 0 && reporters[0] != nil {
		reporter = reporters[0]
	}
	return &ProvisionWorker{runtime: runtime, repository: repository, logger: logger, reporter: reporter}
}

func (w *ProvisionWorker) Handle(ctx context.Context, job *jobs.Job) error {
	log := w.logger.With(ports.LogKeyJobID, job.JobID, ports.LogKeyWorkerType, "server.provision")

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

	log.Info("Provisioning server", ports.LogKeyServerID, spec.ServerID)

	server, err := w.runtime.Deploy(ctx, spec)
	if err != nil {
		log.Error("Failed to provision server", err)
		report("failed", "", "", err.Error())
		return fmt.Errorf("provision failed: %w", err)
	}

	record := &ports.ServerRecord{
		ID:         spec.ServerID,
		Name:       spec.ServerID,
		Status:     server.State,
		NodeID:     server.Labels.NodeID,
		RuntimeRef: server.RuntimeRef,
		ProjectID:  spec.ProjectID,
		EnvironmentID: spec.EnvironmentID,
	}

	if err := w.repository.Save(ctx, record); err != nil {
		log.Error("Failed to store server record", err)
		report("failed", server.RuntimeRef, "", err.Error())
		return fmt.Errorf("failed to store runtime reference: %w", err)
	}

	endpoint, epErr := w.runtime.Endpoint(ctx, spec.ProjectID, spec.ServerID)
	if epErr != nil {
		log.Warn("Failed to resolve endpoint after provision", "workload_id", spec.ServerID, "error", epErr)
	}
	report(strings.ToLower(server.State), server.RuntimeRef, endpoint, "")

	log.Info("Server provisioned successfully", ports.LogKeyServerID, record.ID, "runtime_ref", record.RuntimeRef)
	return nil
}
