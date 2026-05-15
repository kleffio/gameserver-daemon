package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
	"github.com/kleffio/kleff-daemon/pkg/labels"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const labelStrategy = "kleff.io/runtime-strategy"

var gameServerGVR = schema.GroupVersionResource{
	Group:    "agones.dev",
	Version:  "v1",
	Resource: "gameservers",
}

// Adapter is a generic Kubernetes RuntimeAdapter.
// It routes deployments to one of three strategies based on WorkloadSpec.RuntimeHints.KubernetesStrategy:
//
//	"agones"      → Agones GameServer CRD  (game servers)
//	"statefulset" → StatefulSet + PVC      (databases, persistent services)
//	""            → Deployment + Service   (stateless web/app workloads)
type Adapter struct {
	dynamic   dynamic.Interface
	typed     kubernetes.Interface
	namespace string
	nodeID    string
}

func New(kubeconfig, namespace, nodeID string) (*Adapter, error) {
	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		return nil, err
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create typed client: %w", err)
	}

	return &Adapter{dynamic: dyn, typed: typed, namespace: namespace, nodeID: nodeID}, nil
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to build in-cluster config: %w", err)
		}
		return cfg, nil
	}
	if strings.HasPrefix(kubeconfig, "http") {
		return &rest.Config{Host: kubeconfig}, nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}
	return cfg, nil
}

// Deploy provisions and starts a new workload.
func (a *Adapter) Deploy(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	switch spec.RuntimeHints.KubernetesStrategy {
	case "agones":
		return a.deployAgones(ctx, spec)
	case "statefulset":
		return a.deployStatefulSet(ctx, spec)
	default:
		return a.deployDeployment(ctx, spec)
	}
}

// Start resumes a previously stopped workload.
func (a *Adapter) Start(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	switch spec.RuntimeHints.KubernetesStrategy {
	case "agones":
		// Agones GameServers are ephemeral — recreate it.
		return a.deployAgones(ctx, spec)
	case "statefulset":
		return a.scaleStatefulSet(ctx, spec.ServerID, 1)
	default:
		return a.scaleDeployment(ctx, spec.ServerID, 1)
	}
}

// EnsureProjectScope is a no-op on Kubernetes for now. Project isolation will
// be implemented via a per-project namespace once the platform module lands.
func (a *Adapter) EnsureProjectScope(_ context.Context, projectID, projectSlug string) (*ports.ProjectScope, error) {
	return &ports.ProjectScope{
		ProjectID:   projectID,
		ProjectSlug: projectSlug,
		NetworkName: a.namespace,
	}, nil
}

// TeardownProjectScope is a no-op on Kubernetes for now.
func (a *Adapter) TeardownProjectScope(_ context.Context, _ string) error { return nil }

// Stop suspends a workload without removing it.
func (a *Adapter) Stop(ctx context.Context, projectID, workloadID string) error {
	strategy, err := a.strategyFor(ctx, projectID, workloadID)
	if err != nil {
		return err
	}
	switch strategy {
	case "agones":
		return a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Delete(ctx, workloadID, metav1.DeleteOptions{})
	case "statefulset":
		_, err := a.scaleStatefulSet(ctx, workloadID, 0)
		return err
	default:
		_, err := a.scaleDeployment(ctx, workloadID, 0)
		return err
	}
}

// Remove permanently deletes a workload and all associated resources.
func (a *Adapter) Remove(ctx context.Context, projectID, workloadID string) error {
	strategy, err := a.strategyFor(ctx, projectID, workloadID)
	if err != nil {
		return err
	}
	switch strategy {
	case "agones":
		return a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Delete(ctx, workloadID, metav1.DeleteOptions{})
	case "statefulset":
		return a.typed.AppsV1().StatefulSets(a.namespace).Delete(ctx, workloadID, metav1.DeleteOptions{})
	default:
		return a.typed.AppsV1().Deployments(a.namespace).Delete(ctx, workloadID, metav1.DeleteOptions{})
	}
}

// Status returns the current state of a workload.
func (a *Adapter) Status(ctx context.Context, projectID, workloadID string) (*ports.WorkloadHealth, error) {
	strategy, err := a.strategyFor(ctx, projectID, workloadID)
	if err != nil {
		return nil, err
	}
	switch strategy {
	case "agones":
		gs, err := a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Get(ctx, workloadID, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get game server: %w", err)
		}
		state, _, _ := unstructured.NestedString(gs.Object, "status", "state")
		return &ports.WorkloadHealth{WorkloadID: workloadID, State: strings.ToLower(state)}, nil
	default:
		return &ports.WorkloadHealth{WorkloadID: workloadID, State: "unknown"}, nil
	}
}

// Endpoint returns the address users connect to.
func (a *Adapter) Endpoint(ctx context.Context, projectID, workloadID string) (string, error) {
	strategy, err := a.strategyFor(ctx, projectID, workloadID)
	if err != nil {
		return "", err
	}
	if strategy == "agones" {
		gs, err := a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Get(ctx, workloadID, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to get game server: %w", err)
		}
		address, _, _ := unstructured.NestedString(gs.Object, "status", "address")
		ports, _, _ := unstructured.NestedSlice(gs.Object, "status", "ports")
		if len(ports) > 0 {
			if p, ok := ports[0].(map[string]interface{}); ok {
				port := fmt.Sprintf("%v", p["port"])
				return fmt.Sprintf("%s:%s", address, port), nil
			}
		}
		return address, nil
	}
	svc, err := a.typed.CoreV1().Services(a.namespace).Get(ctx, workloadID, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get service: %w", err)
	}
	if len(svc.Spec.Ports) > 0 {
		return fmt.Sprintf("%s:%d", svc.Spec.ClusterIP, svc.Spec.Ports[0].Port), nil
	}
	return svc.Spec.ClusterIP, nil
}

// Logs is not yet implemented for Kubernetes.
func (a *Adapter) Logs(_ context.Context, _ string, _ string, _ bool) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (a *Adapter) Discover(_ context.Context) ([]*ports.ServerRecord, error) {
	return nil, nil
}

// --- Agones strategy ---

func (a *Adapter) deployAgones(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	serverLabels := labels.WorkloadLabels{
		OwnerID:     spec.OwnerID,
		ServerID:    spec.ServerID,
		BlueprintID: spec.BlueprintID,
		NodeID:      a.nodeID,
		ProjectID:   spec.ProjectID,
		ProjectSlug: spec.ProjectSlug,
	}

	labelMap := serverLabels.ToMap()
	labelMap[labelStrategy] = "agones"
	labelInterface := toInterfaceMap(labelMap)

	agonesPortSpecs := make([]interface{}, 0, len(spec.PortRequirements))
	for i, p := range spec.PortRequirements {
		agonesPortSpecs = append(agonesPortSpecs, map[string]interface{}{
			"name":          fmt.Sprintf("port-%d", i),
			"portPolicy":    "Dynamic",
			"containerPort": int64(p.TargetPort),
			"protocol":      strings.ToUpper(p.Protocol),
		})
	}

	gs := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "agones.dev/v1",
			"kind":       "GameServer",
			"metadata": map[string]interface{}{
				"name":      spec.ServerID,
				"namespace": a.namespace,
				"labels":    labelInterface,
			},
			"spec": map[string]interface{}{
				"container": "game",
				"health":    map[string]interface{}{"disabled": true},
				"ports":     agonesPortSpecs,
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"containers": []interface{}{
							map[string]interface{}{
								"name":      "game",
								"image":     spec.Image,
								"env":       buildEnvList(spec.EnvOverrides),
								"resources": buildResourceRequirements(spec.MemoryBytes, spec.CPUMillicores),
								"lifecycle": map[string]interface{}{
									"postStart": map[string]interface{}{
										"exec": map[string]interface{}{
											"command": []interface{}{
												"/bin/sh", "-c",
												"sleep 5 && curl -sf -X POST http://localhost:9358/ready || true",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Create(ctx, gs, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create game server: %w", err)
	}

	return a.waitForAgonesReady(ctx, spec.ServerID, serverLabels)
}

func (a *Adapter) waitForAgonesReady(ctx context.Context, name string, serverLabels labels.WorkloadLabels) (*ports.RunningServer, error) {
	var server *ports.RunningServer

	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		gs, err := a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		state, _, _ := unstructured.NestedString(gs.Object, "status", "state")
		if state != "Ready" {
			return false, nil
		}
		server = &ports.RunningServer{Labels: serverLabels, RuntimeRef: name, State: "Ready"}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return server, nil
}

// --- StatefulSet strategy ---

func (a *Adapter) deployStatefulSet(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	serverLabels := labels.WorkloadLabels{
		OwnerID: spec.OwnerID, ServerID: spec.ServerID, BlueprintID: spec.BlueprintID, NodeID: a.nodeID, ProjectID: spec.ProjectID, ProjectSlug: spec.ProjectSlug,
	}
	selectorLabels := map[string]string{"app": spec.ServerID, labelStrategy: "statefulset", labels.ProjectID: spec.ProjectID, labels.EnvironmentID: spec.EnvironmentID}
	if spec.ProjectSlug != "" {
		selectorLabels[labels.ProjectSlug] = spec.ProjectSlug
	}

	replicas := int32(1)
	sts := buildStatefulSet(spec, replicas, selectorLabels)

	if _, err := a.typed.AppsV1().StatefulSets(a.namespace).Create(ctx, sts, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to create statefulset: %w", err)
	}

	svc := buildService(spec, selectorLabels)
	if _, err := a.typed.CoreV1().Services(a.namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to create service: %w", err)
	}

	return &ports.RunningServer{Labels: serverLabels, RuntimeRef: spec.ServerID, State: "Running"}, nil
}

func (a *Adapter) scaleStatefulSet(ctx context.Context, workloadID string, replicas int32) (*ports.RunningServer, error) {
	scale, err := a.typed.AppsV1().StatefulSets(a.namespace).GetScale(ctx, workloadID, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get statefulset scale: %w", err)
	}
	scale.Spec.Replicas = replicas
	if _, err := a.typed.AppsV1().StatefulSets(a.namespace).UpdateScale(ctx, workloadID, scale, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to scale statefulset: %w", err)
	}
	state := "Running"
	if replicas == 0 {
		state = "Stopped"
	}
	return &ports.RunningServer{RuntimeRef: workloadID, State: state}, nil
}

// --- Deployment strategy ---

func (a *Adapter) deployDeployment(ctx context.Context, spec ports.WorkloadSpec) (*ports.RunningServer, error) {
	serverLabels := labels.WorkloadLabels{
		OwnerID: spec.OwnerID, ServerID: spec.ServerID, BlueprintID: spec.BlueprintID, NodeID: a.nodeID, ProjectID: spec.ProjectID, ProjectSlug: spec.ProjectSlug,
	}
	selectorLabels := map[string]string{"app": spec.ServerID, labelStrategy: "deployment", labels.ProjectID: spec.ProjectID, labels.EnvironmentID: spec.EnvironmentID}
	if spec.ProjectSlug != "" {
		selectorLabels[labels.ProjectSlug] = spec.ProjectSlug
	}

	replicas := int32(1)
	deploy := buildDeployment(spec, replicas, selectorLabels)

	if _, err := a.typed.AppsV1().Deployments(a.namespace).Create(ctx, deploy, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to create deployment: %w", err)
	}

	svc := buildService(spec, selectorLabels)
	if _, err := a.typed.CoreV1().Services(a.namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to create service: %w", err)
	}

	return &ports.RunningServer{Labels: serverLabels, RuntimeRef: spec.ServerID, State: "Running"}, nil
}

func (a *Adapter) scaleDeployment(ctx context.Context, workloadID string, replicas int32) (*ports.RunningServer, error) {
	scale, err := a.typed.AppsV1().Deployments(a.namespace).GetScale(ctx, workloadID, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get deployment scale: %w", err)
	}
	scale.Spec.Replicas = replicas
	if _, err := a.typed.AppsV1().Deployments(a.namespace).UpdateScale(ctx, workloadID, scale, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to scale deployment: %w", err)
	}
	state := "Running"
	if replicas == 0 {
		state = "Stopped"
	}
	return &ports.RunningServer{RuntimeRef: workloadID, State: state}, nil
}

// --- Helpers ---

// strategyFor looks up the runtime strategy label on the live resource.
func (a *Adapter) strategyFor(ctx context.Context, projectID, workloadID string) (string, error) {
	// Try Agones GameServer first.
	gs, err := a.dynamic.Resource(gameServerGVR).Namespace(a.namespace).Get(ctx, workloadID, metav1.GetOptions{})
	if err == nil {
		lbls, _, _ := unstructured.NestedStringMap(gs.Object, "metadata", "labels")
		if err := ensureProjectMatch(projectID, lbls, workloadID); err != nil {
			return "", err
		}
		return lbls[labelStrategy], nil
	}
	// Try StatefulSet.
	sts, err := a.typed.AppsV1().StatefulSets(a.namespace).Get(ctx, workloadID, metav1.GetOptions{})
	if err == nil {
		if err := ensureProjectMatch(projectID, sts.Labels, workloadID); err != nil {
			return "", err
		}
		return sts.Labels[labelStrategy], nil
	}
	// Try Deployment.
	deploy, err := a.typed.AppsV1().Deployments(a.namespace).Get(ctx, workloadID, metav1.GetOptions{})
	if err == nil {
		if err := ensureProjectMatch(projectID, deploy.Labels, workloadID); err != nil {
			return "", err
		}
		return deploy.Labels[labelStrategy], nil
	}
	return "", fmt.Errorf("workload %q not found in any strategy", workloadID)
}

func ensureProjectMatch(projectID string, resourceLabels map[string]string, workloadID string) error {
	if projectID == "" {
		return nil
	}
	gotProject := strings.TrimSpace(resourceLabels[labels.ProjectID])
	gotEnvironment := strings.TrimSpace(resourceLabels[labels.EnvironmentID])
	if gotProject == projectID || gotEnvironment == projectID {
		return nil
	}
	return fmt.Errorf("%w: workload %s belongs to project/environment %q/%q, not %q", ports.ErrProjectMismatch, workloadID, gotProject, gotEnvironment, projectID)
}

func buildEnvList(overrides map[string]string) []interface{} {
	env := make([]interface{}, 0, len(overrides))
	for k, v := range overrides {
		env = append(env, map[string]interface{}{"name": k, "value": v})
	}
	return env
}

func buildEnvVars(overrides map[string]string) []corev1.EnvVar {
	env := make([]corev1.EnvVar, 0, len(overrides))
	for k, v := range overrides {
		env = append(env, corev1.EnvVar{Name: k, Value: v})
	}
	return env
}

func buildResourceRequirements(memBytes, cpuMillicores int64) map[string]interface{} {
	mem := resource.NewQuantity(4*1024*1024*1024, resource.BinarySI)
	if memBytes > 0 {
		mem = resource.NewQuantity(memBytes, resource.BinarySI)
	}
	cpu := resource.NewMilliQuantity(2000, resource.DecimalSI)
	if cpuMillicores > 0 {
		cpu = resource.NewMilliQuantity(cpuMillicores, resource.DecimalSI)
	}
	return map[string]interface{}{
		"requests": map[string]interface{}{"memory": mem.String(), "cpu": cpu.String()},
		"limits":   map[string]interface{}{"memory": mem.String(), "cpu": cpu.String()},
	}
}

func buildTypedResourceRequirements(memBytes, cpuMillicores int64) corev1.ResourceRequirements {
	mem := resource.NewQuantity(4*1024*1024*1024, resource.BinarySI)
	if memBytes > 0 {
		mem = resource.NewQuantity(memBytes, resource.BinarySI)
	}
	cpu := resource.NewMilliQuantity(2000, resource.DecimalSI)
	if cpuMillicores > 0 {
		cpu = resource.NewMilliQuantity(cpuMillicores, resource.DecimalSI)
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: *mem,
			corev1.ResourceCPU:    *cpu,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: *mem,
			corev1.ResourceCPU:    *cpu,
		},
	}
}

func buildServicePorts(portReqs []ports.PortRequirement) []corev1.ServicePort {
	svcPorts := make([]corev1.ServicePort, 0, len(portReqs))
	for i, p := range portReqs {
		protocol := corev1.ProtocolTCP
		if strings.ToLower(p.Protocol) == "udp" {
			protocol = corev1.ProtocolUDP
		}
		svcPorts = append(svcPorts, corev1.ServicePort{
			Name:     fmt.Sprintf("port-%d", i),
			Port:     int32(p.TargetPort),
			Protocol: protocol,
		})
	}
	return svcPorts
}

func toInterfaceMap(m map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func buildStatefulSet(spec ports.WorkloadSpec, replicas int32, selectorLabels map[string]string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:   spec.ServerID,
			Labels: selectorLabels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "app",
							Image:     spec.Image,
							Env:       buildEnvVars(spec.EnvOverrides),
							Resources: buildTypedResourceRequirements(spec.MemoryBytes, spec.CPUMillicores),
						},
					},
				},
			},
		},
	}
}

func buildDeployment(spec ports.WorkloadSpec, replicas int32, selectorLabels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   spec.ServerID,
			Labels: selectorLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:      "app",
							Image:     spec.Image,
							Env:       buildEnvVars(spec.EnvOverrides),
							Resources: buildTypedResourceRequirements(spec.MemoryBytes, spec.CPUMillicores),
						},
					},
				},
			},
		},
	}
}

func buildService(spec ports.WorkloadSpec, selectorLabels map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   spec.ServerID,
			Labels: selectorLabels,
		},
		Spec: corev1.ServiceSpec{
			Selector: selectorLabels,
			Ports:    buildServicePorts(spec.PortRequirements),
		},
	}
}


func (a *Adapter) InjectFile(_ context.Context, _, _, _, _, _, _ string) error {
	return fmt.Errorf("mod file injection is not yet supported on Kubernetes")
}

func (a *Adapter) RemoveFile(_ context.Context, _, _, _, _, _ string) error {
	return fmt.Errorf("mod file removal is not yet supported on Kubernetes")
}

func (a *Adapter) ListRunning(_ context.Context) ([]*ports.ServerRecord, error) {
	return nil, nil
}
