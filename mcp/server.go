package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	mcpgo "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/theodik/lightrun/config"
	"github.com/theodik/lightrun/manager"
)

type Server struct {
	mgr      *manager.Manager
	gateways []config.Gateway
	gwIndex  map[string]config.Gateway
	logger   *slog.Logger
	mcp      *server.MCPServer
	http     *server.StreamableHTTPServer
}

func New(mgr *manager.Manager, gateways []config.Gateway, logger *slog.Logger) *Server {
	idx := make(map[string]config.Gateway, len(gateways))
	for _, g := range gateways {
		idx[g.Name] = g
	}
	s := &Server{
		mgr:      mgr,
		gateways: gateways,
		gwIndex:  idx,
		logger:   logger,
	}
	s.mcp = server.NewMCPServer(
		"lightrun",
		"0.1.0",
		server.WithToolCapabilities(false),
	)
	s.registerTools()
	s.http = server.NewStreamableHTTPServer(s.mcp)
	return s
}

// Run starts the MCP HTTP listener and blocks until ctx is cancelled or the
// listener errors. The MCP streamable transport is served at /mcp and a
// liveness check is served at /health. On cancel it triggers a graceful
// Shutdown (5s grace) of both the HTTP server and the streamable session
// manager.
func (s *Server) Run(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/mcp", s.http)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	s.logger.Info("mcp listening", "addr", addr)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpErr := srv.Shutdown(shutdownCtx)
		sessErr := s.http.Shutdown(shutdownCtx)
		if httpErr != nil {
			return fmt.Errorf("mcp http shutdown: %w", httpErr)
		}
		if sessErr != nil {
			return fmt.Errorf("mcp session shutdown: %w", sessErr)
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) gatewayNames() []string {
	out := make([]string, len(s.gateways))
	for i, g := range s.gateways {
		out[i] = g.Name
	}
	return out
}

func (s *Server) startToolDescription() string {
	var b strings.Builder
	b.WriteString("Start a managed app process. The caller declares the 'port' the app will listen on; lightrun routes the gateway listener to 127.0.0.1:<port> for the given subdomain. The 'gateway' parameter selects which named listener to register against. Each gateway corresponds to a Traefik router with its own middleware chain (auth, public, etc.) — lightrun does not enforce auth itself. The process inherits lightrun's environment (minus LIGHTRUN_* config vars), with PORT=<port> set as a convenience hint and any additional env vars supplied layered on top — if the caller sets PORT in env it overrides the hint (env is passed through verbatim). The app is expected to listen on the declared port; lightrun probes 127.0.0.1:<port> for ~5s after start and reports a 'health' field via the status tool: 'starting' (probe in progress), 'healthy' (connect succeeded), 'unhealthy' (window elapsed without a successful connect — typically means the app bound a different port).\n\nAvailable gateways:\n")
	for _, g := range s.gateways {
		fmt.Fprintf(&b, "  - %s", g.Name)
		if g.Description != "" {
			fmt.Fprintf(&b, ": %s", g.Description)
		}
		fmt.Fprintf(&b, " (URLs: %s)\n", g.URL)
	}
	return b.String()
}

func (s *Server) registerTools() {
	names := s.gatewayNames()

	s.mcp.AddTool(
		mcpgo.NewTool("start",
			mcpgo.WithDescription(s.startToolDescription()),
			mcpgo.WithString("binary_path",
				mcpgo.Required(),
				mcpgo.Description("Absolute path to the executable to launch."),
			),
			mcpgo.WithString("subdomain",
				mcpgo.Required(),
				mcpgo.Description("Subdomain prefix the process will be reachable at (DNS-label-safe, e.g. 'myapp')."),
			),
			mcpgo.WithNumber("port",
				mcpgo.Required(),
				mcpgo.Description("Port (1-65535) the app will listen on. Lightrun routes the gateway to 127.0.0.1:<port> and also exposes it as PORT in the child env."),
			),
			mcpgo.WithString("gateway",
				mcpgo.Required(),
				mcpgo.Enum(names...),
				mcpgo.Description("Which gateway to register against. One of: "+strings.Join(names, ", ")),
			),
			mcpgo.WithObject("env",
				mcpgo.Description("Optional map of additional environment variables (string -> string)."),
			),
		),
		s.handleStart,
	)

	s.mcp.AddTool(
		mcpgo.NewTool("stop",
			mcpgo.WithDescription("Stop a managed process by id. Sends SIGTERM, then SIGKILL after 5 seconds. Logs remain accessible."),
			mcpgo.WithString("id", mcpgo.Required(), mcpgo.Description("Process id returned by start.")),
		),
		s.handleStop,
	)

	s.mcp.AddTool(
		mcpgo.NewTool("restart",
			mcpgo.WithDescription("Restart a managed process by id. Stops the existing process (SIGTERM, then SIGKILL after 5s) and starts a new one with the same binary, subdomain, gateway, and env. The old Process entry stays in the map with status=stopped so its logs and exit info remain accessible by the old id. Returns the new process (with a fresh id)."),
			mcpgo.WithString("id", mcpgo.Required(), mcpgo.Description("Process id to restart.")),
		),
		s.handleRestart,
	)

	s.mcp.AddTool(
		mcpgo.NewTool("status",
			mcpgo.WithDescription("Get status of a single process by id, or a summary of all processes if id is omitted or '*'."),
			mcpgo.WithString("id", mcpgo.Description("Process id, or '*' / omitted for all.")),
		),
		s.handleStatus,
	)

	s.mcp.AddTool(
		mcpgo.NewTool("logs",
			mcpgo.WithDescription("Return the last N lines of combined stdout+stderr from the process's rolling log buffer."),
			mcpgo.WithString("id", mcpgo.Required(), mcpgo.Description("Process id.")),
			mcpgo.WithNumber("lines", mcpgo.Description("Number of lines to return (default 50, max 1000).")),
		),
		s.handleLogs,
	)

	s.mcp.AddTool(
		mcpgo.NewTool("gateways",
			mcpgo.WithDescription("List configured gateways with their URL templates and descriptions. Use this to discover which gateway names are accepted by the 'start' tool and what each one means in this deployment."),
		),
		s.handleGateways,
	)
}

type processView struct {
	ID         string   `json:"id"`
	BinaryPath string   `json:"binary_path"`
	Subdomain  string   `json:"subdomain"`
	URL        string   `json:"url"`
	Port       int      `json:"port"`
	Gateway    string   `json:"gateway"`
	Status     string   `json:"status"`
	Health     string   `json:"health"`
	StartTime  string   `json:"start_time"`
	UptimeSec  int64    `json:"uptime_sec"`
	ExitCode   *int     `json:"exit_code,omitempty"`
	RecentLogs []string `json:"recent_logs,omitempty"`
}

func (s *Server) view(p *manager.Process, includeLogs int) processView {
	url := ""
	if gw, ok := s.gwIndex[p.Gateway]; ok {
		url = fmt.Sprintf(gw.URL, p.Subdomain)
	}
	v := processView{
		ID:         p.ID,
		BinaryPath: p.BinaryPath,
		Subdomain:  p.Subdomain,
		URL:        url,
		Port:       p.Port,
		Gateway:    p.Gateway,
		Status:     p.Status().String(),
		Health:     p.Health().String(),
		StartTime:  p.StartTime.UTC().Format(time.RFC3339),
		UptimeSec:  int64(time.Since(p.StartTime).Seconds()),
	}
	if p.Status() == manager.StatusStopped {
		code, _ := p.ExitInfo()
		v.ExitCode = &code
	}
	if includeLogs > 0 {
		v.RecentLogs = p.Tail(includeLogs)
	}
	return v
}

func (s *Server) handleStart(_ context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()

	binaryPath, _ := args["binary_path"].(string)
	subdomain, _ := args["subdomain"].(string)
	gateway, _ := args["gateway"].(string)

	portRaw, ok := args["port"]
	if !ok {
		return mcpgo.NewToolResultError("port is required"), nil
	}
	portF, ok := portRaw.(float64)
	if !ok {
		return mcpgo.NewToolResultError("port must be a number"), nil
	}
	port := int(portF)
	if float64(port) != portF || port <= 0 || port > 65535 {
		return mcpgo.NewToolResultError(fmt.Sprintf("port must be an integer in 1-65535, got %v", portRaw)), nil
	}

	if gateway == "" {
		return mcpgo.NewToolResultError("gateway is required"), nil
	}
	if _, ok := s.gwIndex[gateway]; !ok {
		return mcpgo.NewToolResultError(fmt.Sprintf("unknown gateway %q; valid: %s", gateway, strings.Join(s.gatewayNames(), ", "))), nil
	}

	env := map[string]string{}
	if raw, ok := args["env"].(map[string]any); ok {
		for k, v := range raw {
			if sv, ok := v.(string); ok {
				env[k] = sv
			} else {
				return mcpgo.NewToolResultError(fmt.Sprintf("env[%q] must be a string", k)), nil
			}
		}
	}

	p, err := s.mgr.Start(manager.StartOptions{
		BinaryPath: binaryPath,
		Subdomain:  subdomain,
		Port:       port,
		Env:        env,
		Gateway:    gateway,
	})
	if err != nil {
		return mcpgo.NewToolResultError(err.Error()), nil
	}

	return jsonResult(s.view(p, 0))
}

func (s *Server) handleStop(_ context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	id, _ := req.GetArguments()["id"].(string)
	if id == "" {
		return mcpgo.NewToolResultError("id is required"), nil
	}
	if err := s.mgr.Stop(id); err != nil {
		return mcpgo.NewToolResultError(err.Error()), nil
	}
	return mcpgo.NewToolResultText(fmt.Sprintf(`{"id":%q,"stopped":true}`, id)), nil
}

func (s *Server) handleRestart(_ context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	id, _ := req.GetArguments()["id"].(string)
	if id == "" {
		return mcpgo.NewToolResultError("id is required"), nil
	}
	p, err := s.mgr.Restart(id)
	if err != nil {
		return mcpgo.NewToolResultError(err.Error()), nil
	}
	return jsonResult(s.view(p, 0))
}

func (s *Server) handleStatus(_ context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	id, _ := req.GetArguments()["id"].(string)
	if id == "" || id == "*" {
		all := s.mgr.List()
		views := make([]processView, 0, len(all))
		for _, p := range all {
			views = append(views, s.view(p, 5))
		}
		return jsonResult(map[string]any{"processes": views})
	}
	p, ok := s.mgr.Get(id)
	if !ok {
		return mcpgo.NewToolResultError(fmt.Sprintf("process %q not found", id)), nil
	}
	return jsonResult(s.view(p, 5))
}

func (s *Server) handleLogs(_ context.Context, req mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	args := req.GetArguments()
	id, _ := args["id"].(string)
	if id == "" {
		return mcpgo.NewToolResultError("id is required"), nil
	}
	lines := 50
	if v, ok := args["lines"].(float64); ok {
		lines = int(v)
	}
	if lines <= 0 {
		lines = 50
	}
	if lines > 1000 {
		lines = 1000
	}

	p, ok := s.mgr.Get(id)
	if !ok {
		return mcpgo.NewToolResultError(fmt.Sprintf("process %q not found", id)), nil
	}
	return jsonResult(map[string]any{
		"id":    id,
		"lines": p.Tail(lines),
	})
}

func (s *Server) handleGateways(_ context.Context, _ mcpgo.CallToolRequest) (*mcpgo.CallToolResult, error) {
	type gatewayView struct {
		Name        string `json:"name"`
		URLTemplate string `json:"url_template"`
		Description string `json:"description,omitempty"`
	}
	views := make([]gatewayView, 0, len(s.gateways))
	for _, g := range s.gateways {
		views = append(views, gatewayView{
			Name:        g.Name,
			URLTemplate: g.URL,
			Description: g.Description,
		})
	}
	return jsonResult(map[string]any{"gateways": views})
}

func jsonResult(v any) (*mcpgo.CallToolResult, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return mcpgo.NewToolResultText(string(b)), nil
}
