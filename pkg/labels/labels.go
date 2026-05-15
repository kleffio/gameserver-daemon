package labels

const (
	OwnerID     = "kleff.io/owner_id"
	WorkloadID  = "kleff.io/workload_id"
	ServerID    = "kleff.io/server_id" // Deprecated: use WorkloadID; kept for reconcile during rollout
	BlueprintID = "kleff.io/blueprint_id"
	NodeID      = "kleff.io/node_id"

	// Phase 1: NamespaceID is the new authoritative tenancy label.
	// New containers get this label; legacy containers only have ProjectID.
	NamespaceID = "kleff.io/namespace_id"

	// Legacy labels — preserved for dual-read compat.
	// Old containers have these; new containers get NamespaceID instead.
	ProjectID     = "kleff.io/project_id"
	EnvironmentID = "kleff.io/environment_id"
	ProjectSlug   = "kleff.io/project_slug"
	ManagedBy     = "kleff.io/managed_by"

	ManagedByValue = "kleff-daemon"

	// ComposeProject is the standard Docker Compose label used by Portainer to group containers.
	ComposeProject      = "com.docker.compose.project"
	ComposeProjectValue = "kleff-local"
)

type WorkloadLabels struct {
	OwnerID       string
	ServerID      string
	BlueprintID   string
	NodeID        string
	NamespaceID   string // Phase 1: primary tenancy key
	ProjectID     string // Legacy fallback
	EnvironmentID string
	ProjectSlug   string
}

func (l *WorkloadLabels) ToMap() map[string]string {
	m := map[string]string{
		OwnerID:        l.OwnerID,
		WorkloadID:     l.ServerID, // new key
		ServerID:       l.ServerID, // deprecated alias — kept during transition
		BlueprintID:    l.BlueprintID,
		NodeID:         l.NodeID,
		ManagedBy:      ManagedByValue,
		ComposeProject: ComposeProjectValue,
	}
	if l.NamespaceID != "" {
		m[NamespaceID] = l.NamespaceID
	}
	if l.ProjectID != "" {
		m[ProjectID] = l.ProjectID
	}
	if l.EnvironmentID != "" {
		m[EnvironmentID] = l.EnvironmentID
	}
	if l.ProjectSlug != "" {
		m[ProjectSlug] = l.ProjectSlug
	}
	return m
}

// EffectiveNamespaceID returns the namespace ID for authorization.
// It prefers the new namespace_id label and falls back to project_id
// for legacy containers whose labels cannot be updated (Docker immutability).
func (l *WorkloadLabels) EffectiveNamespaceID() string {
	if l.NamespaceID != "" {
		return l.NamespaceID
	}
	return l.ProjectID // legacy fallback
}

func FromMap(m map[string]string) WorkloadLabels {
	if m[ManagedBy] != ManagedByValue {
		return WorkloadLabels{}
	}
	// Accept containers labeled with either the new workload_id key or the old server_id key.
	workloadID := m[WorkloadID]
	if workloadID == "" {
		workloadID = m[ServerID]
	}
	return WorkloadLabels{
		OwnerID:       m[OwnerID],
		ServerID:      workloadID,
		BlueprintID:   m[BlueprintID],
		NodeID:        m[NodeID],
		NamespaceID:   m[NamespaceID],
		ProjectID:     m[ProjectID],
		EnvironmentID: m[EnvironmentID],
		ProjectSlug:   m[ProjectSlug],
	}
}
