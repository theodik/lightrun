package web

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/theodik/lightrun/config"
	"github.com/theodik/lightrun/manager"
)

func TestCommandLogsUseLatestStoppedProcess(t *testing.T) {
	mgr := manager.New(20, 0, "")
	h := New(mgr, testGateways(), nil)

	p, err := mgr.Start(manager.StartOptions{
		Command:   "printf 'boot failed\\n'",
		Name:      "Failing command",
		Subdomain: "cmdlogs",
		Port:      freeTestPort(t),
		Gateway:   "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, p, manager.StatusStopped)

	req := httptest.NewRequest(http.MethodGet, "/api/commands/cmdlogs/logs?lines=20", nil)
	rr := httptest.NewRecorder()
	h.handleCommandLogs(rr, req, "cmdlogs")

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	var body struct {
		Lines []string `json:"lines"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Lines) != 1 || body.Lines[0] != "boot failed" {
		t.Fatalf("lines = %#v, want boot failed", body.Lines)
	}
}

func TestCommandEventsInitialPatchIncludesLogs(t *testing.T) {
	mgr := manager.New(20, 0, "")
	h := New(mgr, testGateways(), nil)

	p, err := mgr.Start(manager.StartOptions{
		Command:   "printf 'ready\\n'",
		Name:      "Ready command",
		Subdomain: "cmdevents",
		Port:      freeTestPort(t),
		Gateway:   "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, p, manager.StatusStopped)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/commands/cmdevents/events", nil).WithContext(ctx)
	rr := httptest.NewRecorder()
	h.handleCommandEvents(rr, req, "cmdevents")

	body := rr.Body.String()
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, body)
	}
	if !strings.Contains(body, "event: datastar-patch-elements") {
		t.Fatalf("SSE patch event missing: %s", body)
	}
	if !strings.Contains(body, "log-output") || !strings.Contains(body, "ready") {
		t.Fatalf("log patch missing ready output: %s", body)
	}
}

func TestCommandRemoveStopsAndUnregisters(t *testing.T) {
	mgr := manager.New(20, 0, "")
	h := New(mgr, testGateways(), nil)

	p, err := mgr.Start(manager.StartOptions{
		Command:   "sleep 30",
		Name:      "Remove command",
		Subdomain: "cmdremove",
		Port:      freeTestPort(t),
		Gateway:   "g",
	})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/commands/cmdremove/remove", nil)
	rr := httptest.NewRecorder()
	h.handleCommandRemove(rr, req, "cmdremove")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if p.Status() != manager.StatusStopped {
		t.Fatalf("process status = %s, want stopped", p.Status())
	}
	if _, ok := mgr.GetDesired("cmdremove"); ok {
		t.Fatal("command still registered after remove")
	}
}

func TestServesLocalDatastarAsset(t *testing.T) {
	mgr := manager.New(20, 0, "")
	h := New(mgr, testGateways(), nil)

	req := httptest.NewRequest(http.MethodGet, "/static/vendor/datastar.js", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Datastar v1.0.2") {
		t.Fatal("asset body does not look like datastar bundle")
	}
}

func testGateways() []config.Gateway {
	return []config.Gateway{{Name: "g", Port: 18080, URL: "https://%s.example.test"}}
}

func freeTestPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForStatus(t *testing.T, p *manager.Process, want manager.Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Status() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status = %s, want %s", p.Status(), want)
}
