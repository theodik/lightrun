package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/theodik/lightrun/manager"
)

var (
	issuedPortsMu sync.Mutex
	issuedPorts   = map[int]bool{}
)

func freePort(t *testing.T) int {
	t.Helper()
	issuedPortsMu.Lock()
	defer issuedPortsMu.Unlock()
	for try := 0; try < 64; try++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if !issuedPorts[port] {
			issuedPorts[port] = true
			return port
		}
	}
	t.Fatal("could not allocate a unique freePort")
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

func newTestManager(t *testing.T) *manager.Manager {
	t.Helper()
	m := manager.New(10, 0)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		m.StopAll(ctx)
	})
	return m
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestServeHTTP_MissingSubdomain(t *testing.T) {
	m := newTestManager(t)
	s := New(m, "g1", 0, discardLogger())
	req := httptest.NewRequest("GET", "http://foo/", nil)
	req.Host = "foo"
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestServeHTTP_UnknownSubdomain(t *testing.T) {
	m := newTestManager(t)
	s := New(m, "g1", 0, discardLogger())
	req := httptest.NewRequest("GET", "http://ghost.app.example.com/", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestServeHTTP_WrongGateway(t *testing.T) {
	m := newTestManager(t)
	bin := writeSleepBin(t)
	if _, err := m.Start(manager.StartOptions{
		BinaryPath: bin, Subdomain: "beta", Port: freePort(t), Gateway: "g2",
	}); err != nil {
		t.Fatal(err)
	}

	// Proxy is for g1, but the process is on g2 → expect 404, not a proxy attempt.
	s := New(m, "g1", 0, discardLogger())
	req := httptest.NewRequest("GET", "http://beta.app.example.com/", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestServeHTTP_ForwardsToBackend(t *testing.T) {
	port := freePort(t)
	m := newTestManager(t)
	bin := writeSleepBin(t)

	// Register a manager process at `port` — the sleep binary never binds, so
	// the OS-level port stays free for the backend listener below.
	if _, err := m.Start(manager.StartOptions{
		BinaryPath: bin, Subdomain: "alpha", Port: port, Gateway: "g1",
	}); err != nil {
		t.Fatal(err)
	}

	// Bind a real HTTP backend on the same port; the proxy's reverse-proxy
	// target is 127.0.0.1:<port>, which is exactly what we serve here.
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", "test")
		fmt.Fprintf(w, "hello %s", r.URL.Path)
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		t.Fatal(err)
	}
	backend.Listener = ln
	backend.Start()
	t.Cleanup(backend.Close)

	s := New(m, "g1", 0, discardLogger())
	req := httptest.NewRequest("GET", "http://alpha.app.example.com/ping", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get("X-Backend"); got != "test" {
		t.Errorf("X-Backend = %q, want test (proxy did not reach upstream)", got)
	}
	if !strings.Contains(w.Body.String(), "hello /ping") {
		t.Errorf("body = %q, want %q", w.Body.String(), "hello /ping")
	}
}

func TestServeHTTP_BadGatewayWhenBackendDown(t *testing.T) {
	// The shell process is registered (status=running) but nothing is bound
	// at its declared port. The reverse proxy's Dial fails → ErrorHandler →
	// 502 with "upstream error".
	m := newTestManager(t)
	bin := writeSleepBin(t)
	if _, err := m.Start(manager.StartOptions{
		BinaryPath: bin, Subdomain: "dead", Port: freePort(t), Gateway: "g1",
	}); err != nil {
		t.Fatal(err)
	}

	s := New(m, "g1", 0, discardLogger())
	req := httptest.NewRequest("GET", "http://dead.app.example.com/", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
	if !strings.Contains(w.Body.String(), "upstream error") {
		t.Errorf("body = %q, want contains 'upstream error'", w.Body.String())
	}
}

func TestExtractSubdomain(t *testing.T) {
	cases := []struct {
		name string
		host string
		want string
	}{
		{"normal", "foo.app.example.com", "foo"},
		{"with port", "foo.app.example.com:443", "foo"},
		{"trailing dot", "foo.app.example.com.", "foo"},
		{"uppercase normalised", "FOO.App.example.com", "foo"},
		{"two label host", "a.b", "a"},
		{"no dot", "foo", ""},
		{"empty", "", ""},
		{"leading dot", ".foo.bar", ""},
		{"only port", ":443", ""},
		{"two label with port", "foo.bar:80", "foo"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := extractSubdomain(c.host)
			if got != c.want {
				t.Errorf("extractSubdomain(%q) = %q, want %q", c.host, got, c.want)
			}
		})
	}
}
