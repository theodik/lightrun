package web

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/theodik/lightrun/config"
	"github.com/theodik/lightrun/manager"
)

//go:embed static
var staticFiles embed.FS

type processView struct {
	ID         string `json:"id"`
	BinaryPath string `json:"binary_path"`
	WorkingDir string `json:"working_dir"`
	Subdomain  string `json:"subdomain"`
	Name       string `json:"name,omitempty"`
	URL        string `json:"url,omitempty"`
	Port       int    `json:"port"`
	Gateway    string `json:"gateway"`
	Status     string `json:"status"`
	Health     string `json:"health"`
	StartTime  string `json:"start_time"`
	UptimeSec  int64  `json:"uptime_sec"`
	ExitCode   *int   `json:"exit_code,omitempty"`
}

type commandView struct {
	Subdomain   string `json:"subdomain"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	DisplayCmd  string `json:"display_cmd"` // Command string if set, else BinaryPath
	Gateway     string `json:"gateway"`
	Port        int    `json:"port"`
	Running     bool   `json:"running"`
	ProcessID   string `json:"process_id,omitempty"`
	Status      string `json:"status"`
	Health      string `json:"health"`
	URL         string `json:"url,omitempty"`
	UptimeSec   int64  `json:"uptime_sec,omitempty"`
}

type gatewayView struct {
	Name        string   `json:"name"`
	URLTemplate string   `json:"url_template"`
	Description string   `json:"description,omitempty"`
	Subdomains  []string `json:"subdomains,omitempty"`
}

// Handler serves the browser UI and its small JSON/SSE API.
type Handler struct {
	mgr      *manager.Manager
	gateways []config.Gateway
	gwIndex  map[string]config.Gateway
	logger   *slog.Logger
	index    []byte
	start    []byte
	command  []byte
	assets   http.Handler
	full     *template.Template
	cmdFull  *template.Template
}

// New creates a web UI handler.
func New(mgr *manager.Manager, gateways []config.Gateway, logger *slog.Logger) *Handler {
	idx := make(map[string]config.Gateway, len(gateways))
	for _, g := range gateways {
		idx[g.Name] = g
	}
	index, err := fs.ReadFile(staticFiles, "static/index.html")
	if err != nil {
		panic(err)
	}
	start, err := fs.ReadFile(staticFiles, "static/start.html")
	if err != nil {
		panic(err)
	}
	command, err := fs.ReadFile(staticFiles, "static/command.html")
	if err != nil {
		panic(err)
	}
	staticRoot, err := fs.Sub(staticFiles, "static")
	if err != nil {
		panic(err)
	}
	return &Handler{
		mgr:      mgr,
		gateways: gateways,
		gwIndex:  idx,
		logger:   logger,
		index:    index,
		start:    start,
		command:  command,
		assets:   http.StripPrefix("/static/", http.FileServer(http.FS(staticRoot))),
		full:     template.Must(template.New("full").Parse(fullUpdateTmpl)),
		cmdFull:  template.Must(template.New("cmdFull").Parse(commandUpdateTmpl)),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(h.index)
	case r.URL.Path == "/start" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(h.start)
	case strings.HasPrefix(r.URL.Path, "/commands/") && r.Method == http.MethodGet:
		// Static HTML shell — JS reads the subdomain from the URL.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(h.command)
	case strings.HasPrefix(r.URL.Path, "/static/") && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		h.assets.ServeHTTP(w, r)
	case r.URL.Path == "/api/events" && r.Method == http.MethodGet:
		h.handleEvents(w, r)
	case r.URL.Path == "/api/processes" && r.Method == http.MethodGet:
		h.handleListProcesses(w, r)
	case r.URL.Path == "/api/processes" && r.Method == http.MethodPost:
		h.handleStartProcess(w, r)
	case r.URL.Path == "/api/gateways" && r.Method == http.MethodGet:
		h.handleGateways(w, r)
	case r.URL.Path == "/api/commands" && r.Method == http.MethodGet:
		h.handleListCommands(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/commands/"):
		h.handleCommandAction(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/processes/"):
		h.handleProcessAction(w, r)
	default:
		http.NotFound(w, r)
	}
}

// handleCommandAction dispatches /api/commands/:subdomain[/action].
func (h *Handler) handleCommandAction(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/commands/")
	subdomain, action, hasAction := strings.Cut(rest, "/")
	if subdomain == "" {
		http.NotFound(w, r)
		return
	}

	if !hasAction {
		// GET /api/commands/:subdomain
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		h.handleGetCommand(w, r, subdomain)
		return
	}

	switch {
	case action == "run" && r.Method == http.MethodPost:
		h.handleRunCommand(w, r, subdomain)
	case action == "stop" && r.Method == http.MethodPost:
		h.handleCommandStop(w, r, subdomain)
	case action == "restart" && r.Method == http.MethodPost:
		h.handleCommandRestart(w, r, subdomain)
	case action == "remove" && r.Method == http.MethodPost:
		h.handleCommandRemove(w, r, subdomain)
	case action == "logs" && r.Method == http.MethodGet:
		h.handleCommandLogs(w, r, subdomain)
	case action == "events" && r.Method == http.MethodGet:
		h.handleCommandEvents(w, r, subdomain)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleListCommands(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"commands": h.commandViews()})
}

func (h *Handler) handleGetCommand(w http.ResponseWriter, _ *http.Request, subdomain string) {
	opts, ok := h.mgr.GetDesired(subdomain)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "command not found"})
		return
	}
	cv := h.buildCommandView(opts)
	writeJSON(w, http.StatusOK, map[string]any{"command": cv})
}

func (h *Handler) handleRunCommand(w http.ResponseWriter, _ *http.Request, subdomain string) {
	p, alreadyRunning, err := h.mgr.RunCommand(subdomain)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"process":         h.view(p),
		"already_running": alreadyRunning,
	})
}

func (h *Handler) handleCommandStop(w http.ResponseWriter, _ *http.Request, subdomain string) {
	p, ok := h.mgr.GetBySubdomain(subdomain)
	if !ok || p.Status() != manager.StatusRunning {
		writeJSON(w, http.StatusOK, map[string]string{"status": "not running"})
		return
	}
	if err := h.mgr.StopInstance(p.ID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (h *Handler) handleCommandRestart(w http.ResponseWriter, _ *http.Request, subdomain string) {
	// If running, restart via its process ID. Otherwise, run from the registry.
	running, ok := h.mgr.GetBySubdomain(subdomain)
	if ok && running.Status() == manager.StatusRunning {
		p, err := h.mgr.Restart(running.ID)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"process": h.view(p)})
		return
	}
	p, _, err := h.mgr.RunCommand(subdomain)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"process": h.view(p)})
}

func (h *Handler) handleCommandRemove(w http.ResponseWriter, _ *http.Request, subdomain string) {
	if err := h.mgr.UnregisterCommand(subdomain); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (h *Handler) handleCommandLogs(w http.ResponseWriter, r *http.Request, subdomain string) {
	p, ok := h.mgr.LatestBySubdomain(subdomain)
	if !ok {
		// Command exists but has no current or retained process logs.
		if _, desired := h.mgr.GetDesired(subdomain); desired {
			writeJSON(w, http.StatusOK, map[string]any{"lines": []string{}})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "command not found"})
		return
	}

	lines := 200
	if raw := r.URL.Query().Get("lines"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			if n > 1000 {
				n = 1000
			}
			lines = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": p.ID, "lines": p.Tail(lines)})
}

func (h *Handler) handleCommandEvents(w http.ResponseWriter, r *http.Request, subdomain string) {
	if _, ok := h.mgr.GetDesired(subdomain); !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "command not found"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	h.sendCommandUpdate(w, subdomain)
	flusher.Flush()

	ch, unsubscribe := h.mgr.Subscribe()
	defer unsubscribe()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case e := <-ch:
			if e.Kind == manager.EventKindRegistry && e.ID == subdomain {
				if _, ok := h.mgr.GetDesired(subdomain); !ok {
					h.sendCommandDeleted(w, subdomain)
					flusher.Flush()
					return
				}
				h.sendCommandUpdate(w, subdomain)
				flusher.Flush()
				continue
			}
			if e.Process != nil && e.Process.Subdomain == subdomain {
				h.sendCommandUpdate(w, subdomain)
				flusher.Flush()
			}
		}
	}
}

func (h *Handler) handleProcessAction(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/processes/")
	id, action, ok := strings.Cut(rest, "/")
	if !ok || id == "" {
		http.NotFound(w, r)
		return
	}
	switch {
	case action == "stop" && r.Method == http.MethodPost:
		if err := h.mgr.Stop(id); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case action == "restart" && r.Method == http.MethodPost:
		p, err := h.mgr.Restart(id)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, h.view(p))
	case action == "logs" && r.Method == http.MethodGet:
		h.handleLogs(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleListProcesses(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"processes": h.allProcessViews()})
}

func (h *Handler) handleStartProcess(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var opts manager.StartOptions
	if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	p, err := h.mgr.Start(opts)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, h.view(p))
}

func (h *Handler) handleLogs(w http.ResponseWriter, r *http.Request, id string) {
	p, ok := h.mgr.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "process not found"})
		return
	}
	lines := 50
	if raw := r.URL.Query().Get("lines"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid lines"})
			return
		}
		if n > 1000 {
			n = 1000
		}
		lines = n
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "lines": p.Tail(lines)})
}

func (h *Handler) handleGateways(w http.ResponseWriter, _ *http.Request) {
	views := make([]gatewayView, 0, len(h.gateways))
	for _, g := range h.gateways {
		views = append(views, gatewayView{
			Name:        g.Name,
			URLTemplate: g.URL,
			Description: g.Description,
			Subdomains:  g.Subdomains,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"gateways": views})
}

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	h.sendUpdate(w)
	flusher.Flush()

	ch, unsubscribe := h.mgr.Subscribe()
	defer unsubscribe()

	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case e := <-ch:
			if e.Kind == manager.EventKindLog {
				continue
			}
			h.sendUpdate(w)
			flusher.Flush()
		}
	}
}

func (h *Handler) sendUpdate(w http.ResponseWriter) {
	var b bytes.Buffer
	data := map[string]any{
		"Commands":  h.commandViews(),
		"Processes": h.adhocViews(),
	}
	if err := h.full.Execute(&b, data); err != nil {
		h.logger.Error("render update", "err", err)
		return
	}
	writeDatastarPatch(w, b.String())
}

func (h *Handler) sendCommandUpdate(w http.ResponseWriter, subdomain string) {
	opts, ok := h.mgr.GetDesired(subdomain)
	if !ok {
		return
	}
	lines := []string{}
	if p, ok := h.mgr.LatestBySubdomain(subdomain); ok {
		lines = p.Tail(300)
	}
	var b bytes.Buffer
	data := map[string]any{
		"Command": h.buildCommandView(opts),
		"Lines":   lines,
	}
	if err := h.cmdFull.Execute(&b, data); err != nil {
		h.logger.Error("render command update", "subdomain", subdomain, "err", err)
		return
	}
	writeDatastarPatch(w, b.String())
}

func (h *Handler) sendCommandDeleted(w http.ResponseWriter, subdomain string) {
	html := `<div id="cmd-heading"><h1 id="cmd-name">Command removed</h1><span id="status-badge" class="badge badge-stopped">removed</span></div>
<div id="cmd-meta"><p id="cmd-description" class="description">This command is no longer registered.</p></div>
<div id="cmd-actions" class="actions"><a href="/" role="button" class="secondary">Dashboard</a></div>
<pre id="log-output">Command removed.</pre>`
	writeDatastarPatch(w, html)
}

func writeDatastarPatch(w http.ResponseWriter, html string) {
	_, _ = fmt.Fprint(w, "event: datastar-patch-elements\n")
	for _, line := range strings.Split(html, "\n") {
		_, _ = fmt.Fprintf(w, "data: elements %s\n", line)
	}
	_, _ = fmt.Fprint(w, "\n")
}

// commandViews returns views for all named registry entries.
func (h *Handler) commandViews() []commandView {
	desired := h.mgr.ListDesired()
	views := make([]commandView, 0, len(desired))
	for _, opts := range desired {
		if opts.Name == "" {
			continue // unnamed: shown in the processes section
		}
		views = append(views, h.buildCommandView(opts))
	}
	return views
}

func (h *Handler) buildCommandView(opts manager.StartOptions) commandView {
	cv := commandView{
		Subdomain:   opts.Subdomain,
		Name:        opts.Name,
		Description: opts.Description,
		DisplayCmd:  opts.Command,
		Gateway:     opts.Gateway,
		Port:        opts.Port,
		Status:      "stopped",
		Health:      "-",
	}
	if cv.Name == "" {
		cv.Name = opts.Subdomain
	}
	if cv.DisplayCmd == "" {
		cv.DisplayCmd = opts.BinaryPath
	}
	if gw, ok := h.gwIndex[opts.Gateway]; ok {
		cv.URL = fmt.Sprintf(gw.URL, opts.Subdomain)
	}
	if p, ok := h.mgr.GetBySubdomain(opts.Subdomain); ok && p.Status() == manager.StatusRunning {
		cv.Running = true
		cv.ProcessID = p.ID
		cv.Status = p.Status().String()
		cv.Health = p.Health().String()
		cv.UptimeSec = int64(time.Since(p.StartTime).Seconds())
	}
	return cv
}

// adhocViews returns process views for unnamed (ad-hoc) processes only.
func (h *Handler) adhocViews() []processView {
	all := h.mgr.List()
	// Build set of named subdomains so we can exclude them.
	desired := h.mgr.ListDesired()
	named := make(map[string]struct{}, len(desired))
	for _, opts := range desired {
		if opts.Name != "" {
			named[opts.Subdomain] = struct{}{}
		}
	}
	views := make([]processView, 0)
	for _, p := range all {
		if _, isNamed := named[p.Subdomain]; isNamed {
			continue
		}
		views = append(views, h.view(p))
	}
	sort.Slice(views, func(i, j int) bool {
		return views[i].StartTime > views[j].StartTime
	})
	return views
}

// allProcessViews returns views for all processes (for the JSON API).
func (h *Handler) allProcessViews() []processView {
	procs := h.mgr.List()
	views := make([]processView, 0, len(procs))
	for _, p := range procs {
		views = append(views, h.view(p))
	}
	return views
}

func (h *Handler) view(p *manager.Process) processView {
	url := ""
	if gw, ok := h.gwIndex[p.Gateway]; ok {
		url = fmt.Sprintf(gw.URL, p.Subdomain)
	}
	v := processView{
		ID:         p.ID,
		BinaryPath: p.BinaryPath,
		WorkingDir: p.WorkingDir,
		Subdomain:  p.Subdomain,
		Name:       p.Name,
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
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// fullUpdateTmpl renders both the command list and the ad-hoc process table in
// one datastar patch so a single SSE event updates both sections.
const fullUpdateTmpl = `<div id="command-list">
{{- if .Commands}}
{{- range .Commands}}
<div class="cmd-item{{if .Running}} cmd-running{{end}}" id="cmd-{{.Subdomain}}">
  <div class="cmd-info">
    <strong class="cmd-name">{{.Name}}</strong>
    <code class="cmd-command">{{.DisplayCmd}}</code>
  </div>
  <div class="cmd-actions">
    {{if .Running}}<span class="badge badge-running">running</span><a href="/commands/{{.Subdomain}}" class="btn-view">View</a>
    {{else}}<button class="btn-run" data-subdomain="{{.Subdomain}}">Run ▶</button>{{end}}
  </div>
</div>
{{- end}}
{{- else}}
<p class="empty">No commands registered. Use MCP or <a href="/start">Start Process</a> to register one.</p>
{{- end}}
</div>
<tbody id="process-rows">
{{- if .Processes}}
{{- range .Processes}}
<tr id="proc-{{.ID}}">
  <td><strong>{{.Subdomain}}</strong><br><small>{{.ID}}</small></td>
  <td>{{if .Gateway}}{{.Gateway}}{{else}}-{{end}}</td>
  <td>{{if .Port}}{{.Port}}{{else}}-{{end}}</td>
  <td><span class="badge badge-{{.Status}}">{{.Status}}</span></td>
  <td><span class="badge badge-{{.Health}}">{{.Health}}</span></td>
  <td>{{if .URL}}<a href="{{.URL}}" target="_blank" rel="noreferrer">open</a>{{else}}-{{end}}</td>
  <td>{{.UptimeSec}}s</td>
  <td class="actions">
    <button type="button" class="secondary" data-action="logs" data-id="{{.ID}}">Logs</button>
    <button type="button" class="secondary" data-action="restart" data-id="{{.ID}}"{{if eq .Status "stopped"}} disabled{{end}}>Restart</button>
    <button type="button" class="contrast" data-action="stop" data-id="{{.ID}}"{{if eq .Status "stopped"}} disabled{{end}}>Stop</button>
  </td>
</tr>
<tr id="logs-{{.ID}}" class="logs-row" hidden>
  <td colspan="8"><pre id="log-text-{{.ID}}">No logs loaded.</pre></td>
</tr>
{{- end}}
{{- else}}
<tr id="empty-row"><td colspan="8" class="empty">No ad-hoc processes.</td></tr>
{{- end}}
</tbody>`
const commandUpdateTmpl = `<div id="cmd-heading">
  <h1 id="cmd-name">{{.Command.Name}}</h1>
  <span id="status-badge" class="badge badge-{{.Command.Status}}">{{.Command.Status}}</span>
  <span id="health-badge" class="badge badge-{{.Command.Health}}">{{.Command.Health}}</span>
</div>
<div id="cmd-meta">
  {{if .Command.Description}}<p id="cmd-description" class="description">{{.Command.Description}}</p>{{else}}<p id="cmd-description" class="description" hidden></p>{{end}}
  <div class="meta"><code id="cmd-string" class="cmd-string">{{.Command.DisplayCmd}}</code></div>
</div>
<div id="cmd-actions" class="actions">
  <button id="btn-run" type="button" data-command-action="run"{{if .Command.Running}} disabled{{end}}>Run ▶</button>
  <button id="btn-stop" type="button" class="secondary" data-command-action="stop"{{if not .Command.Running}} disabled{{end}}>Stop</button>
  <button id="btn-restart" type="button" class="secondary" data-command-action="restart">Restart</button>
  <button id="btn-remove" type="button" class="contrast" data-command-action="remove">Remove</button>
  {{if .Command.URL}}<a id="btn-open" class="secondary{{if not .Command.Running}} disabled{{end}}" href="{{.Command.URL}}" target="_blank" rel="noreferrer" role="button">Open ↗</a>{{end}}
</div>
<pre id="log-output">{{if .Lines}}{{range $i, $line := .Lines}}{{if $i}}
{{end}}{{$line}}{{end}}{{else}}No logs yet.{{end}}</pre>`
