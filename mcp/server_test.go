package mcp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"

	"github.com/theodik/lightrun/config"
	"github.com/theodik/lightrun/manager"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	gws := []config.Gateway{
		{Name: "g1", Port: 19090, URL: "https://%s.g1.example.com", Description: "first"},
		{Name: "g2", Port: 19091, URL: "https://%s.g2.example.com"},
	}
	mgr := manager.New(20, 0, "")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		mgr.StopAll(ctx)
	})
	return New(mgr, gws, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

var (
	issuedPortsMu sync.Mutex
	issuedPorts   = map[int]bool{}
)

// freePort returns an OS-assigned ephemeral port and remembers it, so a
// rapid close-and-reissue from the kernel can't hand back a port that another
// in-flight test process has already declared.
func freePort(t *testing.T) int {
	t.Helper()
	issuedPortsMu.Lock()
	defer issuedPortsMu.Unlock()
	for try := 0; try < 64; try++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if !issuedPorts[p] {
			issuedPorts[p] = true
			return p
		}
	}
	t.Fatal("could not allocate a unique freePort after 64 attempts")
	return 0
}

func writeSleepBin(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bin.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func callTool(args map[string]any) mcpgo.CallToolRequest {
	return mcpgo.CallToolRequest{Params: mcpgo.CallToolParams{Arguments: args}}
}

// resultText extracts the text payload from a CallToolResult, failing the test
// if the result has no text content (every handler in this package returns
// either NewToolResultError or jsonResult, both text-based).
func resultText(t *testing.T, res *mcpgo.CallToolResult) string {
	t.Helper()
	if res == nil {
		t.Fatal("nil result")
	}
	if len(res.Content) == 0 {
		t.Fatal("no content in result")
	}
	tc, ok := mcpgo.AsTextContent(res.Content[0])
	if !ok {
		t.Fatalf("first content not text, got %T", res.Content[0])
	}
	return tc.Text
}

// ---------- tool registration ----------

func TestRegisteredTools(t *testing.T) {
	s := newTestServer(t)
	got := s.mcp.ListTools()
	want := []string{"start", "stop", "restart", "status", "logs", "gateways"}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("missing tool %q", name)
		}
	}
	if len(got) != len(want) {
		extra := []string{}
		for n := range got {
			extra = append(extra, n)
		}
		sort.Strings(extra)
		t.Errorf("tool set = %v, want exactly %v", extra, want)
	}
}

func TestStartTool_Schema(t *testing.T) {
	s := newTestServer(t)
	st := s.mcp.GetTool("start")
	if st == nil {
		t.Fatal("start tool not registered")
	}

	// JSON round-trip avoids depending on the internal Go types of the schema's
	// Properties map (which can hold []string vs []any depending on builder).
	b, err := json.Marshal(st.Tool.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Type string   `json:"type"`
			Enum []string `json:"enum,omitempty"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatalf("could not parse schema JSON: %v\n%s", err, b)
	}

	required := map[string]bool{}
	for _, r := range schema.Required {
		required[r] = true
	}
	for _, name := range []string{"binary_path", "subdomain", "port", "gateway"} {
		if !required[name] {
			t.Errorf("expected %q to be required, got Required=%v", name, schema.Required)
		}
	}
	if _, ok := schema.Properties["env"]; !ok {
		t.Errorf("expected optional env property to be declared")
	}
	if schema.Properties["port"].Type != "number" {
		t.Errorf("port type = %q, want number", schema.Properties["port"].Type)
	}
	// Gateway must be constrained to the configured names (enum) so the AI
	// can't invent a gateway. Contract documented in project memory.
	if got := schema.Properties["gateway"].Enum; len(got) != 2 {
		t.Errorf("gateway enum = %v, want size 2", got)
	}
}

// ---------- handleStart validation ----------

func TestHandleStart_PortValidation(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	ctx := context.Background()
	base := func() map[string]any {
		return map[string]any{
			"binary_path": bin,
			"subdomain":   "a",
			"gateway":     "g1",
		}
	}

	cases := []struct {
		name string
		args map[string]any
		want string
	}{
		{"missing", base(), "port is required"},
		{"non-number", merge(base(), "port", "8080"), "port must be a number"},
		{"fractional", merge(base(), "port", 8080.5), "integer in 1-65535"},
		{"zero", merge(base(), "port", float64(0)), "integer in 1-65535"},
		{"out of range", merge(base(), "port", float64(70000)), "integer in 1-65535"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := s.handleStart(ctx, callTool(c.args))
			if err != nil {
				t.Fatal(err)
			}
			if !res.IsError {
				t.Errorf("expected IsError=true, got success: %s", resultText(t, res))
				return
			}
			if !strings.Contains(resultText(t, res), c.want) {
				t.Errorf("error %q does not contain %q", resultText(t, res), c.want)
			}
		})
	}
}

func TestHandleStart_GatewayValidation(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	res, err := s.handleStart(context.Background(), callTool(map[string]any{
		"binary_path": bin, "subdomain": "a", "gateway": "nope", "port": float64(freePort(t)),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "unknown gateway") {
		t.Errorf("expected unknown-gateway error, got %q", resultText(t, res))
	}
}

func TestHandleStart_EnvNonStringValueRejected(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	res, err := s.handleStart(context.Background(), callTool(map[string]any{
		"binary_path": bin, "subdomain": "a", "gateway": "g1", "port": float64(freePort(t)),
		"env": map[string]any{"K": 1},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "must be a string") {
		t.Errorf("expected env-string error, got %q", resultText(t, res))
	}
}

func TestHandleStart_HappyPathReturnsView(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	port := freePort(t)

	res, err := s.handleStart(context.Background(), callTool(map[string]any{
		"binary_path": bin, "subdomain": "alpha", "gateway": "g1", "port": float64(port),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %s", resultText(t, res))
	}
	var view processView
	if err := json.Unmarshal([]byte(resultText(t, res)), &view); err != nil {
		t.Fatalf("could not parse view JSON: %v\n%s", err, resultText(t, res))
	}
	if view.Subdomain != "alpha" || view.Gateway != "g1" || view.Port != port {
		t.Errorf("unexpected view: %+v", view)
	}
	if view.URL != "https://alpha.g1.example.com" {
		t.Errorf("URL = %q, want template substitution", view.URL)
	}
	if view.Status != "running" {
		t.Errorf("status = %q, want running", view.Status)
	}
}

// ---------- handleStop ----------

func TestHandleStop_IDRequired(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleStop(context.Background(), callTool(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "id is required") {
		t.Errorf("expected id-required error, got %q", resultText(t, res))
	}
}

func TestHandleStop_NotFound(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleStop(context.Background(), callTool(map[string]any{"id": "ghost"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "not found") {
		t.Errorf("expected not-found error, got %q", resultText(t, res))
	}
}

func TestHandleStop_HappyPath(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	p, err := s.mgr.Start(manager.StartOptions{
		BinaryPath: bin, Subdomain: "sp", Port: freePort(t), Gateway: "g1",
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := s.handleStop(context.Background(), callTool(map[string]any{"id": p.ID}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %s", resultText(t, res))
	}
	if !strings.Contains(resultText(t, res), `"stopped":true`) {
		t.Errorf("response %q does not confirm stop", resultText(t, res))
	}
	if p.Status() != manager.StatusStopped {
		t.Errorf("process status = %s, want stopped", p.Status())
	}
}

// ---------- handleRestart ----------

func TestHandleRestart_IDRequired(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleRestart(context.Background(), callTool(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "id is required") {
		t.Errorf("expected id-required error, got %q", resultText(t, res))
	}
}

func TestHandleRestart_NotFound(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleRestart(context.Background(), callTool(map[string]any{"id": "ghost"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "not found") {
		t.Errorf("expected not-found error, got %q", resultText(t, res))
	}
}

func TestHandleRestart_ReturnsNewView(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	port := freePort(t)
	orig, err := s.mgr.Start(manager.StartOptions{
		BinaryPath: bin, Subdomain: "rs", Port: port, Gateway: "g1",
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := s.handleRestart(context.Background(), callTool(map[string]any{"id": orig.ID}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected error: %s", resultText(t, res))
	}
	var view processView
	if err := json.Unmarshal([]byte(resultText(t, res)), &view); err != nil {
		t.Fatalf("could not parse view: %v\n%s", err, resultText(t, res))
	}
	if view.ID == orig.ID {
		t.Errorf("restart returned same id %q; expected fresh", view.ID)
	}
	if view.Subdomain != "rs" || view.Port != port || view.Gateway != "g1" {
		t.Errorf("restart view did not preserve opts: %+v", view)
	}
}

// ---------- handleStatus ----------

func TestHandleStatus_StarReturnsAll(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	if _, err := s.mgr.Start(manager.StartOptions{BinaryPath: bin, Subdomain: "x", Port: freePort(t), Gateway: "g1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.mgr.Start(manager.StartOptions{BinaryPath: bin, Subdomain: "y", Port: freePort(t), Gateway: "g1"}); err != nil {
		t.Fatal(err)
	}

	res, err := s.handleStatus(context.Background(), callTool(map[string]any{"id": "*"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("error: %s", resultText(t, res))
	}
	var got struct {
		Processes []processView `json:"processes"`
	}
	if err := json.Unmarshal([]byte(resultText(t, res)), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Processes) != 2 {
		t.Errorf("got %d processes, want 2", len(got.Processes))
	}
}

func TestHandleStatus_NotFound(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleStatus(context.Background(), callTool(map[string]any{"id": "ghost"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "not found") {
		t.Errorf("expected not-found error, got %q", resultText(t, res))
	}
}

// ---------- handleLogs ----------

func TestHandleLogs_IDRequired(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleLogs(context.Background(), callTool(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsError || !strings.Contains(resultText(t, res), "id is required") {
		t.Errorf("expected id-required error, got %q", resultText(t, res))
	}
}

func TestHandleLogs_LinesCapAndDefault(t *testing.T) {
	s := newTestServer(t)
	bin := writeSleepBin(t)
	p, err := s.mgr.Start(manager.StartOptions{BinaryPath: bin, Subdomain: "lg", Port: freePort(t), Gateway: "g1"})
	if err != nil {
		t.Fatal(err)
	}

	// Default lines (no arg) — no error and well-formed payload.
	res, err := s.handleLogs(context.Background(), callTool(map[string]any{"id": p.ID}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("default lines errored: %s", resultText(t, res))
	}
	// Over-cap lines (>1000) — silently capped; should still succeed.
	res, err = s.handleLogs(context.Background(), callTool(map[string]any{"id": p.ID, "lines": float64(9999)}))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Errorf("over-cap lines errored: %s", resultText(t, res))
	}
}

// ---------- handleGateways ----------

func TestHandleGateways(t *testing.T) {
	s := newTestServer(t)
	res, err := s.handleGateways(context.Background(), callTool(nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatal(resultText(t, res))
	}
	var got struct {
		Gateways []struct {
			Name        string `json:"name"`
			URLTemplate string `json:"url_template"`
			Description string `json:"description"`
		} `json:"gateways"`
	}
	if err := json.Unmarshal([]byte(resultText(t, res)), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Gateways) != 2 {
		t.Fatalf("got %d gateways, want 2", len(got.Gateways))
	}
	names := []string{got.Gateways[0].Name, got.Gateways[1].Name}
	sort.Strings(names)
	if names[0] != "g1" || names[1] != "g2" {
		t.Errorf("gateway names = %v, want [g1 g2]", names)
	}
	for _, g := range got.Gateways {
		if g.Name == "g1" && g.Description != "first" {
			t.Errorf("g1 description = %q, want first", g.Description)
		}
	}
}

// merge returns a shallow copy of m with key k set to v. Used to build test
// arg maps from a base without mutating the base.
func merge(m map[string]any, k string, v any) map[string]any {
	out := make(map[string]any, len(m)+1)
	for kk, vv := range m {
		out[kk] = vv
	}
	out[k] = v
	return out
}
