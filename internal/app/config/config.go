package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type QueueBackend string

const (
	QueueBackendMemory QueueBackend = "memory"
	QueueBackendRedis  QueueBackend = "redis"
)

type Config struct {
	// Kubeconfig is optional. If empty and a Kubernetes environment is detected,
	// the daemon uses in-cluster config. If set, it can be a kubeconfig file path
	// or an API server URL (e.g. http://localhost:8001).
	Kubeconfig    string       `mapstructure:"kubeconfig"`
	KubeNamespace string       `mapstructure:"kube.namespace"`
	ClusterRegion string       `mapstructure:"cluster.region"`
	NodeID        string       `mapstructure:"node.id"`
	GRPCPort      int          `mapstructure:"grpc.port"`
	MetricsPort            int          `mapstructure:"metrics.port"`
	MetricsScrapeInterval int          `mapstructure:"metrics.scrape_interval"`
	QueueBackend  QueueBackend `mapstructure:"queue.backend"`
	DatabasePath  string       `mapstructure:"database.path"`
	RedisURL      string       `mapstructure:"redis.url"`
	RedisPassword string       `mapstructure:"redis.password"`
	RedisTLS      bool         `mapstructure:"redis.tls"`
	PlatformURL   string       `mapstructure:"platform.url"`
	SharedSecret  string       `mapstructure:"shared_secret"`
	StoragePath   string       `mapstructure:"storage.path"`
	FileAPIPort   int          `mapstructure:"file_api.port"`
	FileAPIURL    string       `mapstructure:"file_api.url"`
}

func (c *Config) Validate() error {
	switch c.QueueBackend {
	case QueueBackendMemory, QueueBackendRedis:
		// valid
	default:
		return fmt.Errorf("invalid queue.backend: %q (must be 'memory' or 'redis')", c.QueueBackend)
	}

	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("node.id is required and cannot be empty")
	}

	if strings.TrimSpace(c.PlatformURL) == "" {
		return fmt.Errorf("KLEFF_PLATFORM_URL is required and cannot be empty")
	}

	if strings.TrimSpace(c.SharedSecret) == "" {
		return fmt.Errorf("KLEFF_SHARED_SECRET is required and cannot be empty")
	}

	return nil
}

func Load() (*Config, error) {
	v := viper.New()

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-node"
	}

	v.SetDefault("kubeconfig", "")
	v.SetDefault("kube.namespace", "default")
	v.SetDefault("cluster.region", "local")
	v.SetDefault("node.id", hostname)
	v.SetDefault("grpc.port", 50051)
	v.SetDefault("metrics.port", 9090)
	v.SetDefault("metrics.scrape_interval", 30)
	v.SetDefault("queue.backend", string(QueueBackendMemory))
	v.SetDefault("database.path", "./data/kleff.db")
	v.SetDefault("redis.url", "redis://localhost:6379/0")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.tls", false)
	v.SetDefault("platform.url", "")
	v.SetDefault("shared_secret", "")
	v.SetDefault("storage.path", "/var/lib/kleffd/servers")
	v.SetDefault("file_api.port", 8083)
	v.SetDefault("file_api.url", "")

	v.SetEnvPrefix("kleff")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath("/etc/gameserver-daemon/")
	v.AddConfigPath(".")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("fatal error config file: %w", err)
		}
	}

	fs := pflag.NewFlagSet("kleff", pflag.ContinueOnError)
	fs.ParseErrorsWhitelist.UnknownFlags = true

	fs.String("kubeconfig", v.GetString("kubeconfig"), "Path to kubeconfig file, or API server URL (empty = auto-detect)")
	fs.String("kube.namespace", v.GetString("kube.namespace"), "Kubernetes namespace to deploy workloads into")
	fs.String("cluster.region", v.GetString("cluster.region"), "Cluster region this daemon belongs to")
	fs.String("node.id", v.GetString("node.id"), "Unique identifier for this daemon node")
	fs.Int("grpc.port", v.GetInt("grpc.port"), "Port for the gRPC server to listen on")
	fs.Int("metrics.port", v.GetInt("metrics.port"), "Port for Prometheus metrics exposure")
	fs.String("queue.backend", v.GetString("queue.backend"), "Backend for the message queue (e.g. memory, redis)")

	fs.Parse(os.Args[1:])
	v.BindPFlags(fs)

	var cfg Config
	cfg.Kubeconfig = v.GetString("kubeconfig")
	cfg.KubeNamespace = v.GetString("kube.namespace")
	cfg.ClusterRegion = v.GetString("cluster.region")
	cfg.NodeID = v.GetString("node.id")
	cfg.GRPCPort = v.GetInt("grpc.port")
	cfg.MetricsPort = v.GetInt("metrics.port")
	cfg.MetricsScrapeInterval = v.GetInt("metrics.scrape_interval")
	cfg.QueueBackend = QueueBackend(v.GetString("queue.backend"))
	cfg.DatabasePath = v.GetString("database.path")
	cfg.RedisURL = v.GetString("redis.url")
	cfg.RedisPassword = v.GetString("redis.password")
	cfg.RedisTLS = v.GetBool("redis.tls")
	cfg.PlatformURL = v.GetString("platform.url")
	cfg.SharedSecret = v.GetString("shared_secret")
	cfg.StoragePath = v.GetString("storage.path")
	cfg.FileAPIPort = v.GetInt("file_api.port")
	cfg.FileAPIURL = v.GetString("file_api.url")

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &cfg, nil
}
