package ports

import (
	"context"
	"errors"
	"io"
)

// ErrProjectMismatch is returned when an operation targets a workload that
// exists but whose project label does not match the expected project.
// Callers should treat this as a hard authorization failure.
var ErrProjectMismatch = errors.New("workload project mismatch")

// ProjectScope describes the per-project runtime resources (e.g. a Docker
// bridge network, a k8s namespace) that workloads in a given project share.
type ProjectScope struct {
	ProjectID   string
	ProjectSlug string
	NetworkName string // Docker network name, or k8s namespace, etc.
}

// RuntimeAdapter is the generic interface for deploying workloads on any backend.
type RuntimeAdapter interface {
	// EnsureProjectScope creates the per-project runtime resources (network,
	// namespace, etc.) if they do not already exist. Must be idempotent.
	EnsureProjectScope(ctx context.Context, projectID, projectSlug string) (*ProjectScope, error)
	// TeardownProjectScope removes the per-project runtime resources. Safe to
	// call when no workloads remain in the project.
	TeardownProjectScope(ctx context.Context, projectID string) error

	// Deploy provisions and starts a new workload from scratch.
	Deploy(ctx context.Context, spec WorkloadSpec) (*RunningServer, error)
	// Start resumes a previously stopped workload. The full spec is required
	// because some backends (e.g. Agones) are stateless and need it to recreate resources.
	Start(ctx context.Context, spec WorkloadSpec) (*RunningServer, error)
	// Stop suspends a running workload without removing it.
	Stop(ctx context.Context, projectID, workloadID string) error
	// Remove permanently deletes a workload and all its resources.
	Remove(ctx context.Context, projectID, workloadID string) error
	// Status returns the current health and resource usage of a workload.
	Status(ctx context.Context, projectID, workloadID string) (*WorkloadHealth, error)
	// Endpoint returns the host:port address players/users connect to.
	Endpoint(ctx context.Context, projectID, workloadID string) (string, error)
	// Logs streams the workload's stdout/stderr.
	Logs(ctx context.Context, projectID, workloadID string, follow bool) (io.ReadCloser, error)

	// InjectFile downloads a file from downloadURL and writes it into the
	// workload's persistent volume under the appropriate content-type subdirectory.
	// storagePath is the volume's mount point inside the container (e.g. "/data").
	InjectFile(ctx context.Context, projectID, workloadID, storagePath, contentType, downloadURL, fileName string) error

	// RemoveFile deletes a previously injected file from the workload's volume.
	RemoveFile(ctx context.Context, projectID, workloadID, storagePath, contentType, fileName string) error

	// Discover returns all workloads currently managed by this runtime node.
	// Called on daemon startup to re-populate the server repository after a restart.
	Discover(ctx context.Context) ([]*ServerRecord, error)
}
