package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dockeradapter "github.com/kleffio/kleff-daemon/internal/adapters/out/runtime/docker"
	k8sadapter "github.com/kleffio/kleff-daemon/internal/adapters/out/runtime/kubernetes"
	"github.com/kleffio/kleff-daemon/internal/application/ports"
)

// testprovision provisions a Minecraft server via either the Kubernetes or Docker adapter.
//
// Kubernetes usage (on the cluster node):
//
//	kubectl proxy --port=8888 &
//	./testprovision k8s
//	./testprovision k8s cleanup
//
// Docker usage (local):
//
//	./testprovision docker
//	./testprovision docker cleanup
func main() {
	const (
		nodeID      = "test-node"
		serverID    = "kleff-test-minecraft"
		ownerID     = "test-owner"
		blueprintID = "minecraft-vanilla"
		projectID   = "test-project"
		projectSlug = "test"
	)

	mode := "k8s"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	cleanup := len(os.Args) > 2 && os.Args[2] == "cleanup"

	spec := ports.WorkloadSpec{
		OwnerID:       ownerID,
		ServerID:      serverID,
		BlueprintID:   blueprintID,
		ProjectID:     projectID,
		ProjectSlug:   projectSlug,
		Image:         "itzg/minecraft-server",
		MemoryBytes:   2048 * 1024 * 1024,
		CPUMillicores: 500,
		EnvOverrides: map[string]string{
			"EULA":        "TRUE",
			"TYPE":        "VANILLA",
			"VERSION":     "LATEST",
			"DIFFICULTY":  "normal",
			"MAX_PLAYERS": "20",
			"MEMORY":      "2G",
		},
		PortRequirements: []ports.PortRequirement{
			{TargetPort: 25565, Protocol: "tcp"},
			{TargetPort: 25565, Protocol: "udp"},
			{TargetPort: 25575, Protocol: "tcp"},
		},
		RuntimeHints: ports.RuntimeHints{
			PersistentStorage: true,
			StoragePath:       "/data",
		},
	}

	switch mode {
	case "docker":
		runDocker(cleanup, serverID, spec)
	default:
		spec.RuntimeHints.KubernetesStrategy = "agones"
		spec.RuntimeHints.ExposeUDP = true
		runK8s(cleanup, serverID, spec)
	}
}

func runDocker(cleanup bool, serverID string, spec ports.WorkloadSpec) {
	adapter, err := dockeradapter.New("test-node", "/var/lib/kleffd/servers", "")
	if err != nil {
		log.Fatalf("failed to create docker adapter: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if cleanup {
		fmt.Printf("Removing %s...\n", serverID)
		if err := adapter.Remove(ctx, spec.ProjectID, serverID); err != nil {
			log.Fatalf("remove failed: %v", err)
		}
		fmt.Println("Removed.")
		return
	}

	fmt.Printf("Provisioning %s (%s) via Docker...\n", serverID, spec.Image)
	fmt.Printf("  Memory: %d MB  CPU: %dm\n", spec.MemoryBytes/1024/1024, spec.CPUMillicores)
	fmt.Printf("  Ports: 25565/tcp, 25565/udp, 25575/tcp\n")
	fmt.Printf("  Storage: %s\n", spec.RuntimeHints.StoragePath)
	fmt.Println("  Pulling image and starting container...")

	start := time.Now()
	server, err := adapter.Deploy(ctx, spec)
	if err != nil {
		log.Fatalf("deploy failed: %v", err)
	}

	fmt.Printf("\nServer is running! (took %s)\n", time.Since(start).Round(time.Second))
	fmt.Printf("  RuntimeRef : %s\n", server.RuntimeRef)
	fmt.Printf("  State      : %s\n", server.State)

	endpoint, err := adapter.Endpoint(ctx, spec.ProjectID, serverID)
	if err != nil {
		fmt.Printf("  Endpoint   : (could not resolve: %v)\n", err)
	} else {
		fmt.Printf("  Endpoint   : %s\n", endpoint)
	}

	fmt.Printf("\nTo clean up: ./testprovision docker cleanup\n")
}

func runK8s(cleanup bool, serverID string, spec ports.WorkloadSpec) {
	const (
		proxyURL  = "http://localhost:8888"
		namespace = "default"
	)

	adapter, err := k8sadapter.New(proxyURL, namespace, "test-node")
	if err != nil {
		log.Fatalf("failed to create kubernetes adapter: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if cleanup {
		fmt.Printf("Removing %s...\n", serverID)
		if err := adapter.Remove(ctx, spec.ProjectID, serverID); err != nil {
			log.Fatalf("remove failed: %v", err)
		}
		fmt.Println("Removed.")
		return
	}

	fmt.Printf("Provisioning %s (%s) via Agones...\n", serverID, spec.Image)
	fmt.Printf("  Memory: %d MB  CPU: %dm\n", spec.MemoryBytes/1024/1024, spec.CPUMillicores)
	fmt.Printf("  Ports: 25565/tcp, 25565/udp, 25575/tcp\n")
	fmt.Println("  Waiting for GameServer to become Ready (up to 10 min)...")

	start := time.Now()
	server, err := adapter.Deploy(ctx, spec)
	if err != nil {
		log.Fatalf("deploy failed: %v", err)
	}

	fmt.Printf("\nServer is ready! (took %s)\n", time.Since(start).Round(time.Second))
	fmt.Printf("  RuntimeRef : %s\n", server.RuntimeRef)
	fmt.Printf("  State      : %s\n", server.State)
	fmt.Printf("  NodeID     : %s\n", server.Labels.NodeID)
	fmt.Printf("  ServerID   : %s\n", server.Labels.ServerID)

	endpoint, err := adapter.Endpoint(ctx, spec.ProjectID, serverID)
	if err != nil {
		fmt.Printf("  Endpoint   : (could not resolve: %v)\n", err)
	} else {
		fmt.Printf("  Endpoint   : %s\n", endpoint)
	}

	fmt.Printf("\nTo clean up: ./testprovision k8s cleanup\n")
}
