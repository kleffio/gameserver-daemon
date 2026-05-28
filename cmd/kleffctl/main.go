package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	dnet "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/kleffio/kleff-daemon/pkg/labels"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "migrate-to-projects" {
		fmt.Fprintln(os.Stderr, "usage: kleffctl migrate-to-projects --project-id <id> [--project-slug <slug>] [--execute]")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("migrate-to-projects", flag.ExitOnError)
	projectID := fs.String("project-id", "default-project", "Project ID to backfill into unlabeled containers")
	projectSlug := fs.String("project-slug", "default", "Project slug label for migrated containers")
	execute := fs.Bool("execute", false, "Apply migration by recreating containers; default is dry-run")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "parse flags: %v\n", err)
		os.Exit(1)
	}
	if strings.TrimSpace(*projectID) == "" {
		fmt.Fprintln(os.Stderr, "--project-id is required")
		os.Exit(1)
	}

	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		fmt.Fprintf(os.Stderr, "docker client: %v\n", err)
		os.Exit(1)
	}

	projectNetwork := projectNetworkName(*projectID)
	if *execute {
		if err := ensureProjectNetwork(ctx, cli, projectNetwork, *projectID, *projectSlug); err != nil {
			fmt.Fprintf(os.Stderr, "ensure project network: %v\n", err)
			os.Exit(1)
		}
	}

	containers, err := cli.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", labels.ManagedBy+"="+labels.ManagedByValue),
		),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "list containers: %v\n", err)
		os.Exit(1)
	}

	var targets []container.Summary
	for _, c := range containers {
		if c.Labels[labels.ProjectID] != "" {
			continue
		}
		targets = append(targets, c)
	}

	if len(targets) == 0 {
		fmt.Println("No unlabeled managed containers found. Nothing to migrate.")
		return
	}

	fmt.Printf("Found %d unlabeled managed containers.\n", len(targets))
	for _, c := range targets {
		name := strings.TrimPrefix(firstName(c.Names), "/")
		wid := c.Labels[labels.WorkloadID]
		if wid == "" {
			wid = c.Labels[labels.ServerID]
		}
		fmt.Printf("- %s (id=%s workload=%s image=%s)\n", name, shortID(c.ID), wid, c.Image)
	}

	if !*execute {
		fmt.Println("Dry-run complete. Re-run with --execute to apply migration.")
		return
	}

	for _, c := range targets {
		if err := migrateContainer(ctx, cli, c.ID, *projectID, *projectSlug, projectNetwork); err != nil {
			fmt.Fprintf(os.Stderr, "migrate %s failed: %v\n", shortID(c.ID), err)
			os.Exit(1)
		}
	}

	fmt.Printf("Migration complete. Migrated %d container(s).\n", len(targets))
}

func migrateContainer(ctx context.Context, cli *client.Client, containerID, projectID, projectSlug, projectNetwork string) error {
	inspect, err := cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}

	name := strings.TrimPrefix(inspect.Name, "/")
	backupName := fmt.Sprintf("%s-legacy-%s", name, time.Now().UTC().Format("20060102150405"))

	cfg := inspect.Config
	if cfg == nil {
		return fmt.Errorf("missing container config")
	}
	if cfg.Labels == nil {
		cfg.Labels = make(map[string]string)
	}
	cfg.Labels[labels.ManagedBy] = labels.ManagedByValue
	cfg.Labels[labels.ProjectID] = projectID
	if projectSlug != "" {
		cfg.Labels[labels.ProjectSlug] = projectSlug
	}
	if cfg.Labels[labels.WorkloadID] == "" && cfg.Labels[labels.ServerID] != "" {
		cfg.Labels[labels.WorkloadID] = cfg.Labels[labels.ServerID]
	}
	if cfg.Labels[labels.ServerID] == "" && cfg.Labels[labels.WorkloadID] != "" {
		cfg.Labels[labels.ServerID] = cfg.Labels[labels.WorkloadID]
	}

	hostCfg := inspect.HostConfig
	if hostCfg == nil {
		hostCfg = &container.HostConfig{}
	}
	hostCfg.NetworkMode = container.NetworkMode(projectNetwork)

	if inspect.State != nil && inspect.State.Running {
		timeout := 10
		if err := cli.ContainerStop(ctx, inspect.ID, container.StopOptions{Timeout: &timeout}); err != nil {
			return fmt.Errorf("stop old container: %w", err)
		}
	}

	if err := cli.ContainerRename(ctx, inspect.ID, backupName); err != nil {
		return fmt.Errorf("rename old container: %w", err)
	}

	netCfg := &dnet.NetworkingConfig{
		EndpointsConfig: map[string]*dnet.EndpointSettings{
			projectNetwork: {},
		},
	}

	created, err := cli.ContainerCreate(ctx, cfg, hostCfg, netCfg, nil, name)
	if err != nil {
		_ = cli.ContainerRename(ctx, inspect.ID, name)
		return fmt.Errorf("create new container: %w", err)
	}

	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("start new container: %w", err)
	}

	if err := cli.ContainerRemove(ctx, inspect.ID, container.RemoveOptions{Force: true}); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not remove backup container %s: %v\n", backupName, err)
	}
	return nil
}

func ensureProjectNetwork(ctx context.Context, cli *client.Client, name, projectID, projectSlug string) error {
	nets, err := cli.NetworkList(ctx, dnet.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", labels.ProjectID+"="+projectID)),
	})
	if err != nil {
		return err
	}
	if len(nets) > 0 {
		return nil
	}

	netLabels := map[string]string{
		labels.ManagedBy: labels.ManagedByValue,
				labels.ProjectID: projectID,
				labels.EnvironmentID: projectID,
	}
	if projectSlug != "" {
		netLabels[labels.ProjectSlug] = projectSlug
	}
	_, err = cli.NetworkCreate(ctx, name, dnet.CreateOptions{Driver: "bridge", Labels: netLabels})
	return err
}

func projectNetworkName(projectID string) string {
	return "kleff_proj_" + shortID(strings.ReplaceAll(projectID, "-", ""))
}

func shortID(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

func firstName(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[0]
}
