package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
)

type Client struct {
	baseURL      string
	bootstrapKey string
	nodeID       string
	fileAPIURL   string
	nodeToken    string
	httpClient   *http.Client
	logger       ports.Logger
}

func NewClient(baseURL, bootstrapKey, nodeID, fileAPIURL string, logger ports.Logger) *Client {
	return &Client{
		baseURL:      normalizeBaseURL(baseURL),
		bootstrapKey: bootstrapKey,
		nodeID:       nodeID,
		fileAPIURL:   fileAPIURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

func normalizeBaseURL(baseURL string) string {
	baseURL = strings.TrimRight(baseURL, "/")
	baseURL = strings.TrimSuffix(baseURL, "/api/v1")
	return baseURL
}

func (c *Client) RegisterNode(ctx context.Context) error {
	if c.baseURL == "" {
		return fmt.Errorf("platform base url is required")
	}
	if c.bootstrapKey == "" {
		return fmt.Errorf("platform shared secret is required")
	}
	payload := map[string]any{
		"node_id":      c.nodeID,
		"hostname":     c.nodeID,
		"region":       "local",
		"ip_address":   "",
		"file_api_url": c.fileAPIURL,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal node registration payload: %w", err)
	}
	url := c.baseURL + "/api/v1/nodes"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build node registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.bootstrapKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("register node request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("register node failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out struct {
		NodeID    string `json:"node_id"`
		NodeToken string `json:"node_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode node registration response: %w", err)
	}
	if out.NodeToken == "" {
		return fmt.Errorf("node registration response missing node_token")
	}
	c.nodeToken = out.NodeToken
	if out.NodeID != "" {
		c.nodeID = out.NodeID
	}
	c.logger.Info("Registered node with platform", ports.LogKeyNodeID, c.nodeID)
	return nil
}

func (c *Client) ShipLogs(ctx context.Context, workloadID, projectID, environmentID string, lines []ports.LogEntry) error {
	if len(lines) == 0 {
		return nil
	}
	type lineDTO struct {
		Ts     string `json:"ts"`
		Stream string `json:"stream"`
		Line   string `json:"line"`
	}
	dtos := make([]lineDTO, len(lines))
	for i, l := range lines {
		dtos[i] = lineDTO{Ts: l.Ts.UTC().Format(time.RFC3339Nano), Stream: l.Stream, Line: l.Line}
	}
	payload := map[string]any{"project_id": projectID, "environment_id": environmentID, "lines": dtos}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal log payload: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/internal/workloads/%s/log-lines", c.baseURL, workloadID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build log ship request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.nodeToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("log ship request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("log ship failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

func (c *Client) ReportStatus(ctx context.Context, update ports.WorkloadStatusUpdate) error {
	if c.nodeToken == "" {
		return fmt.Errorf("node token is not set; call RegisterNode first")
	}
	payload := map[string]any{
		"status":          update.Status,
		"runtime_ref":     update.RuntimeRef,
		"endpoint":        update.Endpoint,
		"node_id":         update.NodeID,
		"error_message":   update.ErrorMessage,
		"observed_at":     time.Now().UTC().Format(time.RFC3339),
		"cpu_millicores":  update.CPUMillicores,
		"memory_mb":       update.MemoryMB,
		"network_rx_mb":   update.NetworkRxMB,
		"network_tx_mb":   update.NetworkTxMB,
		"disk_read_mb":    update.DiskReadMB,
		"disk_write_mb":   update.DiskWriteMB,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal status payload: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/internal/workloads/%s/status", c.baseURL, update.WorkloadID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build status callback request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.nodeToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("status callback request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("status callback failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}
