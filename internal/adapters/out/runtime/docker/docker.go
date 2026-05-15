package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	dnet "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/pkg/labels"
)

// Adapter is a Docker RuntimeAdapter.
// All three strategies (agones, statefulset, deployment) map to the same
// Docker container lifecycle — the strategy hint is ignored here.
type Adapter struct {
	client           *client.Client
	nodeID           string
	storageLocalPath string // path inside this container where server data is mounted
	storageHostPath  string // corresponding host path passed to Docker for bind mounts
}

var errContainerNotFound = errors.New("container not found")

// New creates a Docker adapter. storageLocalPath is the directory inside the
// daemon container where game server data lives (e.g. /var/lib/kleffd/servers).
// The adapter auto-detects the matching host path by inspecting its own container
// mounts so that bind mount sources are always valid host filesystem paths.
func New(nodeID, storageLocalPath string) (*Adapter, error) {
	c, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %w", err)
	}

	// Default: host path == local path (works when daemon runs directly on host).
	storageHostPath := storageLocalPath
	if hostname, err := os.Hostname(); err == nil {
		if info, err := c.ContainerInspect(context.Background(), hostname); err == nil {
			for _, m := range info.Mounts {
				if m.Destination == storageLocalPath {
					storageHostPath = m.Source
					break
				}
			}
		}
	}

	return &Adapter{
		client:           c,
		nodeID:           nodeID,
		storageLocalPath: storageLocalPath,
		storageHostPath:  storageHostPath,
	}, nil
}

// Ping checks if the Docker daemon is reachable.
func (a *Adapter) Ping(ctx context.Context) error {
	_, err := a.client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("docker daemon unreachable: %w", err)
	}
	return nil
}

// EnsureNamespaceScope creates the per-namespace bridge network (kleff_ns_{nsID})
// if it does not already exist. This is the Phase 1 replacement for per-project
// networking — one bridge per namespace keeps tenants isolated without bridge exhaustion.
func (a *Adapter) EnsureNamespaceScope(ctx context.Context, namespaceID string) (*ports.ProjectScope, error) {
	if namespaceID == "" {
		return nil, fmt.Errorf("namespace id is required")
	}
	name := namespaceNetworkName(namespaceID)

	existing, err := a.client.NetworkList(ctx, dnet.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", labels.NamespaceID+"="+namespaceID)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list namespace networks: %w", err)
	}
	if len(existing) > 0 {
		return &ports.ProjectScope{
			ProjectID:   namespaceID,
			NetworkName: existing[0].Name,
		}, nil
	}

	netLabels := map[string]string{
		labels.ManagedBy:    labels.ManagedByValue,
		labels.NamespaceID: namespaceID,
		labels.NodeID:      a.nodeID,
	}
	if _, err := a.client.NetworkCreate(ctx, name, dnet.CreateOptions{
		Driver: "bridge",
		Labels: netLabels,
	}); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			if isAddressPoolExhaustedError(err) {
				_, _ = a.pruneUnusedProjectNetworks(ctx)
				if _, retryErr := a.client.NetworkCreate(ctx, name, dnet.CreateOptions{
					Driver: "bridge",
					Labels: netLabels,
				}); retryErr != nil && !strings.Contains(retryErr.Error(), "already exists") {
					return nil, fmt.Errorf("failed to create namespace network after prune: %w", retryErr)
				}
			} else {
				return nil, fmt.Errorf("failed to create namespace network: %w", err)
			}
		}
	}
	return &ports.ProjectScope{
		ProjectID:   namespaceID,
		NetworkName: name,
	}, nil
}

// EnsureProjectScope creates the per-project bridge network if it does not
// already exist. The network is the isolation boundary between projects — all
// containers in a project attach to it, and nothing else can reach them.
func (a *Adapter) EnsureProjectScope(ctx context.Context, projectID, projectSlug string) (*ports.ProjectScope, error) {
	if projectID == "" {
		return nil, fmt.Errorf("project id is required")
	}
	name := projectNetworkName(projectID)

	existing, err := a.client.NetworkList(ctx, dnet.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", labels.ProjectID+"="+projectID)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list networks: %w", err)
	}
	if len(existing) > 0 {
		return &ports.ProjectScope{
			ProjectID:   projectID,
			ProjectSlug: projectSlug,
			NetworkName: existing[0].Name,
		}, nil
	}

	netLabels := map[string]string{
		labels.ManagedBy: labels.ManagedByValue,
			labels.ProjectID: projectID,
			labels.EnvironmentID: projectID,
		labels.NodeID:    a.nodeID,
	}
	if projectSlug != "" {
		netLabels[labels.ProjectSlug] = projectSlug
	}
	if _, err := a.client.NetworkCreate(ctx, name, dnet.CreateOptions{
		Driver: "bridge",
		Labels: netLabels,
	}); err != nil {
		// Tolerate race where another worker created it concurrently.
		if !strings.Contains(err.Error(), "already exists") {
			if isAddressPoolExhaustedError(err) {
				// Best effort cleanup for stale project networks from previous runs.
				_, _ = a.pruneUnusedProjectNetworks(ctx)

				if _, retryErr := a.client.NetworkCreate(ctx, name, dnet.CreateOptions{
					Driver: "bridge",
					Labels: netLabels,
				}); retryErr == nil || strings.Contains(retryErr.Error(), "already exists") {
					return &ports.ProjectScope{
						ProjectID:   projectID,
						ProjectSlug: projectSlug,
						NetworkName: name,
					}, nil
				}

				// Final fallback for local/dev environments where Docker exhausted
				// bridge subnets. This keeps provisioning functional.
				if _, inspectErr := a.client.NetworkInspect(ctx, "bridge", dnet.InspectOptions{}); inspectErr == nil {
					return &ports.ProjectScope{
						ProjectID:   projectID,
						ProjectSlug: projectSlug,
						NetworkName: "bridge",
					}, nil
				}
			}

			return nil, fmt.Errorf("failed to create project network: %w", err)
		}
	}

	return &ports.ProjectScope{
		ProjectID:   projectID,
		ProjectSlug: projectSlug,
		NetworkName: name,
	}, nil
}

func isAddressPoolExhaustedError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "all predefined address pools have been fully subnetted") ||
		strings.Contains(lower, "non-overlapping ipv4 address pool")
}

func (a *Adapter) pruneUnusedProjectNetworks(ctx context.Context) (int, error) {
	nets, err := a.client.NetworkList(ctx, dnet.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", labels.ManagedBy+"="+labels.ManagedByValue)),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to list managed networks: %w", err)
	}

	removed := 0
	for _, n := range nets {
		if !strings.HasPrefix(n.Name, "kleff_proj_") {
			continue
		}

		inspect, inspectErr := a.client.NetworkInspect(ctx, n.ID, dnet.InspectOptions{})
		if inspectErr != nil {
			continue
		}
		if len(inspect.Containers) > 0 {
			continue
		}

		if removeErr := a.client.NetworkRemove(ctx, n.ID); removeErr == nil {
			removed++
		}
	}

	return removed, nil
}

// TeardownProjectScope removes the per-project network. Caller is responsible
// for ensuring no containers remain attached.
func (a *Adapter) TeardownProjectScope(ctx context.Context, projectID string) error {
	if projectID == "" {
		return fmt.Errorf("project id is required")
	}
	nets, err := a.client.NetworkList(ctx, dnet.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", labels.ProjectID+"="+projectID)),
	})
	if err != nil {
		return fmt.Errorf("failed to list project networks: %w", err)
	}
	for _, n := range nets {
		if err := a.client.NetworkRemove(ctx, n.ID); err != nil {
			return fmt.Errorf("failed to remove project network %s: %w", n.Name, err)
		}
	}
	return nil
}

// Deploy pulls the image and starts a new container.
func (a *Adapter) Deploy(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	if spec.NamespaceID == "" && spec.ProjectID == "" {
		return nil, fmt.Errorf("workload spec missing namespace_id")
	}

	// Phase 1: prefer namespace-scoped bridge; fall back to project scope for legacy specs.
	var scope *ports.ProjectScope
	var err error
	if spec.NamespaceID != "" {
		scope, err = a.EnsureNamespaceScope(ctx, spec.NamespaceID)
	} else {
		scope, err = a.EnsureProjectScope(ctx, spec.ProjectID, spec.ProjectSlug)
	}
	if err != nil {
		return nil, err
	}

	// Pull image. If the registry is unreachable or denies access, fall back to
	// whatever is already present in the local Docker image cache so that locally
	// built images (e.g. during development) work without a live registry.
	rc, pullErr := a.client.ImagePull(ctx, spec.Image, image.PullOptions{})
	if pullErr != nil {
		if !a.imageExistsLocally(ctx, spec.Image) {
			return nil, fmt.Errorf("failed to pull image %s: %w", spec.Image, pullErr)
		}
		// Image is available locally — proceed without a fresh pull.
	} else {
		if _, err := io.Copy(io.Discard, rc); err != nil {
			rc.Close()
			return nil, fmt.Errorf("failed to pull image %s: %w", spec.Image, err)
		}
		rc.Close()
	}

	containerID, err := a.createContainer(ctx, spec, scope)
	if err != nil {
		return nil, err
	}

	if err := a.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	return &ports.RunningServer{
		Labels: labels.WorkloadLabels{
			OwnerID:     spec.OwnerID,
			ServerID:    spec.ServerID,
			BlueprintID: spec.BlueprintID,
			NodeID:      a.nodeID,
			NamespaceID: spec.NamespaceID,
			ProjectID:   spec.ProjectID,
			ProjectSlug: spec.ProjectSlug,
		},
		RuntimeRef: containerID,
		State:      "Running",
	}, nil
}

// Start restarts a stopped container. If it no longer exists, re-creates it.
func (a *Adapter) Start(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	if spec.NamespaceID == "" && spec.ProjectID == "" {
		return nil, fmt.Errorf("workload spec missing namespace_id")
	}
	// Use EffectiveNamespaceID for authorization: prefers new label, falls back to project_id.
	effectiveID := spec.NamespaceID
	if effectiveID == "" {
		effectiveID = spec.ProjectID
	}
	containerID, err := a.findContainer(ctx, effectiveID, spec.ServerID)
	if err != nil {
		if errors.Is(err, ports.ErrProjectMismatch) {
			return nil, err
		}
		if !errors.Is(err, errContainerNotFound) {
			return nil, err
		}
		// Container gone — re-create it.
		return a.Deploy(ctx, spec)
	}

	if err := a.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	return &ports.RunningServer{RuntimeRef: containerID, State: "Running"}, nil
}

// Stop stops the container without removing it.
func (a *Adapter) Stop(ctx context.Context, projectID, workloadID string) error {
	containerID, err := a.findContainer(ctx, projectID, workloadID)
	if err != nil {
		return err
	}
	timeout := 10
	if err := a.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}
	return nil
}

// Remove stops and removes the container. Data directory is preserved on disk.
func (a *Adapter) Remove(ctx context.Context, projectID, workloadID string) error {
	containerID, err := a.findContainer(ctx, projectID, workloadID)
	if err != nil {
		return err
	}
	timeout := 10
	_ = a.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout})
	if err := a.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}
	return nil
}

// Status returns the current state and resource metrics of the container.
func (a *Adapter) Status(ctx context.Context, projectID, workloadID string) (*ports.WorkloadHealth, error) {
	containerID, err := a.findContainer(ctx, projectID, workloadID)
	if err != nil {
		return nil, err
	}
	info, err := a.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container: %w", err)
	}
	health := &ports.WorkloadHealth{
		WorkloadID: workloadID,
		State:      strings.ToLower(info.State.Status),
	}
	if info.State.Running {
		if err := a.collectStats(ctx, containerID, health); err != nil {
			// Non-fatal: state is already populated; metrics will be zero.
			_ = err
		}
	}
	return health, nil
}

func (a *Adapter) collectStats(ctx context.Context, containerID string, h *ports.WorkloadHealth) error {
	resp, err := a.client.ContainerStats(ctx, containerID, false)
	if err != nil {
		return fmt.Errorf("container stats: %w", err)
	}
	defer resp.Body.Close()

	var stats container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return fmt.Errorf("decode stats: %w", err)
	}

	// CPU: delta-based percentage converted to millicores.
	cpuDelta := stats.CPUStats.CPUUsage.TotalUsage - stats.PreCPUStats.CPUUsage.TotalUsage
	sysDelta := stats.CPUStats.SystemUsage - stats.PreCPUStats.SystemUsage
	numCPUs := uint64(stats.CPUStats.OnlineCPUs)
	if numCPUs == 0 {
		numCPUs = uint64(len(stats.CPUStats.CPUUsage.PercpuUsage))
	}
	if numCPUs == 0 {
		numCPUs = 1
	}
	if sysDelta > 0 && cpuDelta > 0 {
		h.CPUMillicores = int64((float64(cpuDelta) / float64(sysDelta)) * float64(numCPUs) * 1000)
	}

	// Memory.
	h.MemoryMB = int64(stats.MemoryStats.Usage / (1024 * 1024))

	// Network: sum all interfaces.
	var rxBytes, txBytes uint64
	for _, iface := range stats.Networks {
		rxBytes += iface.RxBytes
		txBytes += iface.TxBytes
	}
	h.NetworkRxMB = float64(rxBytes) / (1024 * 1024)
	h.NetworkTxMB = float64(txBytes) / (1024 * 1024)

	// Disk I/O.
	var diskRead, diskWrite uint64
	for _, entry := range stats.BlkioStats.IoServiceBytesRecursive {
		switch strings.ToLower(entry.Op) {
		case "read":
			diskRead += entry.Value
		case "write":
			diskWrite += entry.Value
		}
	}
	h.DiskReadMB = float64(diskRead) / (1024 * 1024)
	h.DiskWriteMB = float64(diskWrite) / (1024 * 1024)

	return nil
}

// Endpoint returns the first exposed host port.
func (a *Adapter) Endpoint(ctx context.Context, projectID, workloadID string) (string, error) {
	containerID, err := a.findContainer(ctx, projectID, workloadID)
	if err != nil {
		return "", err
	}
	info, err := a.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container: %w", err)
	}
	// Sort port keys so we always pick the lowest container port deterministically.
	keys := make([]string, 0, len(info.NetworkSettings.Ports))
	for k := range info.NetworkSettings.Ports {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	for _, k := range keys {
		bindings := info.NetworkSettings.Ports[nat.Port(k)]
		if len(bindings) > 0 && bindings[0].HostPort != "" {
			return fmt.Sprintf("127.0.0.1:%s", bindings[0].HostPort), nil
		}
	}
	return "", fmt.Errorf("no exposed ports found for workload %s", workloadID)
}

// Logs streams the container's stdout/stderr.
// When follow is true, Since is set to the container's last start time so that
// only the logs from the current run are streamed — avoiding replaying the full
// history across restarts before reaching live output.
func (a *Adapter) Logs(ctx context.Context, projectID, workloadID string, follow bool) (io.ReadCloser, error) {
	containerID, err := a.findContainer(ctx, projectID, workloadID)
	if err != nil {
		return nil, err
	}

	opts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
	}
	if follow {
		if info, err := a.client.ContainerInspect(ctx, containerID); err == nil && info.State != nil {
			if t, err := time.Parse(time.RFC3339Nano, info.State.StartedAt); err == nil && !t.IsZero() {
				opts.Since = t.UTC().Format(time.RFC3339Nano)
			}
		}
	}

	rc, err := a.client.ContainerLogs(ctx, containerID, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}
	return rc, nil
}

// Discover lists all kleff-managed containers on this node and returns a ServerRecord for each.
// Called on startup to re-populate the in-memory server repository after a daemon restart.
func (a *Adapter) Discover(ctx context.Context) ([]*ports.ServerRecord, error) {
	containers, err := a.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", labels.ManagedBy+"="+labels.ManagedByValue),
			filters.Arg("label", labels.NodeID+"="+a.nodeID),
		),
	})
	if err != nil {
		return nil, fmt.Errorf("discover containers: %w", err)
	}

	records := make([]*ports.ServerRecord, 0, len(containers))
	for _, c := range containers {
		wl := labels.FromMap(c.Labels)
		if wl.ServerID == "" {
			continue
		}
		// For namespace workloads, ProjectID = NamespaceID (matches provision_worker convention).
		projectID := wl.NamespaceID
		if projectID == "" {
			projectID = wl.ProjectID
		}
		status := "Stopped"
		if c.State == "running" {
			status = "Running"
		}
		records = append(records, &ports.ServerRecord{
			ID:            wl.ServerID,
			Name:          wl.ServerID,
			Status:        status,
			NodeID:        wl.NodeID,
			RuntimeRef:    c.ID,
			ProjectID:     projectID,
			EnvironmentID: wl.EnvironmentID,
		})
	}
	return records, nil
}

// --- Helpers ---

func (a *Adapter) createContainer(ctx context.Context, spec ports.WorkloadSpec, scope *ports.ProjectScope) (string, error) {
	env := make([]string, 0, len(spec.EnvOverrides))
	for k, v := range spec.EnvOverrides {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}
	for _, p := range spec.PortRequirements {
		proto := strings.ToLower(p.Protocol)
		if proto == "" {
			proto = "tcp"
		}
		natPort := nat.Port(fmt.Sprintf("%d/%s", p.TargetPort, proto))
		exposedPorts[natPort] = struct{}{}
		portBindings[natPort] = []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "0"}} // 0 = random host port
	}

	wl := labels.WorkloadLabels{
		OwnerID:       spec.OwnerID,
		ServerID:      spec.ServerID,
		BlueprintID:   spec.BlueprintID,
		NodeID:        a.nodeID,
		NamespaceID:   spec.NamespaceID,
		ProjectID:     spec.ProjectID,
		EnvironmentID: spec.EnvironmentID,
		ProjectSlug:   spec.ProjectSlug,
	}
	containerLabels := wl.ToMap()

	resources := container.Resources{}
	if spec.MemoryBytes > 0 {
		resources.Memory = spec.MemoryBytes
	}
	if spec.CPUMillicores > 0 {
		// Docker uses CPU quota: 1 vCPU = 100000 quota per 100000 period
		resources.CPUQuota = spec.CPUMillicores * 100
		resources.CPUPeriod = 100000
	}

	var mounts []mount.Mount
	if spec.RuntimeHints.PersistentStorage && spec.RuntimeHints.StoragePath != "" {
		localDir := filepath.Join(a.storageLocalPath, projectDataDir(spec.ProjectID, spec.ServerID))
		if err := os.MkdirAll(localDir, 0777); err != nil {
			return "", fmt.Errorf("create server storage directory: %w", err)
		}
		// MkdirAll respects the process umask, so chmod explicitly to ensure
		// any user inside the game server container can write to /data.
		_ = os.Chmod(localDir, 0777)
		hostDir := filepath.Join(a.storageHostPath, projectDataDir(spec.ProjectID, spec.ServerID))
		mounts = append(mounts, mount.Mount{
			Type:   mount.TypeBind,
			Source: hostDir,
			Target: spec.RuntimeHints.StoragePath,
		})
	}

	netConfig := &dnet.NetworkingConfig{
		EndpointsConfig: map[string]*dnet.EndpointSettings{
			scope.NetworkName: {},
		},
	}

	containerConfig := &container.Config{
		Image:        spec.Image,
		Env:          env,
		ExposedPorts: exposedPorts,
		Labels:       containerLabels,
	}
	if spec.Command != "" {
		containerConfig.Cmd = []string{"sh", "-c", spec.Command}
	}

	resp, err := a.client.ContainerCreate(ctx,
		containerConfig,
		&container.HostConfig{
			PortBindings: portBindings,
			Resources:    resources,
			Mounts:       mounts,
		},
		netConfig,
		nil,
		containerName(spec),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create container: %w", err)
	}
	return resp.ID, nil
}

// imageExistsLocally returns true if the image is already present in the local Docker cache.
func (a *Adapter) imageExistsLocally(ctx context.Context, imageName string) bool {
	images, err := a.client.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return false
	}
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if tag == imageName {
				return true
			}
		}
	}
	return false
}


// findContainer looks up a container by workload id and verifies that its
// project label matches the expected project. Returns ErrProjectMismatch if a
// container with the id exists but belongs to a different project — callers
// must treat that as an authorization failure, not a missing workload.
func (a *Adapter) findContainer(ctx context.Context, projectID, workloadID string) (string, error) {
	if workloadID == "" {
		return "", fmt.Errorf("workload id is required")
	}
	containers, err := a.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", labels.WorkloadID+"="+workloadID),
			filters.Arg("label", labels.ManagedBy+"="+labels.ManagedByValue),
		),
	})
	if err != nil {
		return "", fmt.Errorf("failed to list containers: %w", err)
	}
	if len(containers) == 0 {
		// Fall back to the deprecated server_id label during the transition.
		containers, err = a.client.ContainerList(ctx, container.ListOptions{
			All: true,
			Filters: filters.NewArgs(
				filters.Arg("label", labels.ServerID+"="+workloadID),
				filters.Arg("label", labels.ManagedBy+"="+labels.ManagedByValue),
			),
		})
		if err != nil {
			return "", fmt.Errorf("failed to list containers: %w", err)
		}
	}
	if len(containers) == 0 {
		return "", fmt.Errorf("%w for workload %s", errContainerNotFound, workloadID)
	}
	c := containers[0]
	if projectID != "" {
		// Dual-read auth: accept match on namespace_id (new), project_id, or environment_id (legacy).
		gotNS := c.Labels[labels.NamespaceID]
		gotProject := c.Labels[labels.ProjectID]
		gotEnvironment := c.Labels[labels.EnvironmentID]
		if gotNS != projectID && gotProject != projectID && gotEnvironment != projectID {
			return "", fmt.Errorf("%w: workload %s belongs to ns/proj/env %q/%q/%q, not %q",
				ports.ErrProjectMismatch, workloadID, gotNS, gotProject, gotEnvironment, projectID)
		}
	}
	return c.ID, nil
}

// namespaceNetworkName derives a short, deterministic bridge network name for
// the namespace. Format: kleff_ns_{first12charsOfNsID}.
func namespaceNetworkName(namespaceID string) string {
	return "kleff_ns_" + shortID(namespaceID)
}

// projectNetworkName derives a short, deterministic bridge network name from
// the project id. Docker network names are case-sensitive and allow [a-zA-Z0-9_.-].
func projectNetworkName(projectID string) string {
	return "kleff_proj_" + shortID(projectID)
}

// projectDataDir returns a unique directory name for a workload's persistent data.
func projectDataDir(projectID, workloadID string) string {
	return fmt.Sprintf("kleff_proj_%s_%s", shortID(projectID), workloadID)
}

// contentTypeDirs maps a mod content type to its subdirectory inside the storage volume.
var contentTypeDirs = map[string]string{
	"mod":          "mods",
	"plugin":       "plugins",
	"datapack":     "world/datapacks",
	"resourcepack": "resourcepacks",
}

// InjectFile downloads a file from downloadURL directly into the workload's
// bind-mounted storage directory on the host filesystem.
func (a *Adapter) InjectFile(ctx context.Context, projectID, workloadID, storagePath, contentType, downloadURL, fileName string) error {
	subDir, ok := contentTypeDirs[contentType]
	if !ok {
		return fmt.Errorf("%w: unsupported content type %q", ports.ErrPermanent, contentType)
	}

	destDir := filepath.Join(a.storageLocalPath, projectDataDir(projectID, workloadID), subDir)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("create mod directory: %w", err)
	}

	return downloadFile(ctx, downloadURL, filepath.Join(destDir, fileName))
}

// RemoveFile deletes a previously injected file from the workload's storage directory.
func (a *Adapter) RemoveFile(ctx context.Context, projectID, workloadID, storagePath, contentType, fileName string) error {
	subDir, ok := contentTypeDirs[contentType]
	if !ok {
		return fmt.Errorf("%w: unsupported content type %q", ports.ErrPermanent, contentType)
	}

	filePath := filepath.Join(a.storageLocalPath, projectDataDir(projectID, workloadID), subDir, fileName)
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove file: %w", err)
	}
	return nil
}

func downloadFile(ctx context.Context, url, destPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download: HTTP %d", resp.StatusCode)
	}
	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		os.Remove(destPath)
		return fmt.Errorf("write file: %w", err)
	}
	return nil
}

func shortID(s string) string {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) > 12 {
		s = s[:12]
	}
	return s
}

// containerName builds a human-readable container name in the form
// username.projectslug.servername, falling back gracefully when fields are absent.
func containerName(spec ports.WorkloadSpec) string {
	var parts []string
	if spec.OwnerUsername != "" {
		parts = append(parts, sanitizeNamePart(spec.OwnerUsername))
	}
	if spec.ProjectSlug != "" {
		parts = append(parts, sanitizeNamePart(spec.ProjectSlug))
	}
	if spec.ServerName != "" {
		parts = append(parts, sanitizeNamePart(spec.ServerName))
	}
	if len(parts) == 0 {
		return spec.ServerID
	}
	return strings.Join(parts, ".")
}

// sanitizeNamePart strips characters not allowed in Docker container names.
var nameReplacer = strings.NewReplacer(" ", "-", "@", "-", "/", "-")

func sanitizeNamePart(s string) string {
	s = nameReplacer.Replace(strings.ToLower(strings.TrimSpace(s)))
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			out = append(out, c)
		}
	}
	return strings.Trim(string(out), "-.")
}
