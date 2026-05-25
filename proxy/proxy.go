package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/theodik/lightrun/manager"
)

// Server is a single reverse-proxy listener bound to one named gateway.
// Requests reaching this listener are only routed to processes registered
// against the same gateway name.
type Server struct {
	mgr     *manager.Manager
	name    string
	port    int
	logger  *slog.Logger
}

func New(mgr *manager.Manager, name string, port int, logger *slog.Logger) *Server {
	return &Server{mgr: mgr, name: name, port: port, logger: logger}
}

// Run starts the proxy listener and blocks until ctx is cancelled or the
// listener errors. On cancel it triggers a graceful Shutdown (5s grace).
func (s *Server) Run(ctx context.Context) error {
	addr := ":" + strconv.Itoa(s.port)
	srv := &http.Server{Addr: addr, Handler: s}
	s.logger.Info("proxy listening", "addr", addr)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("proxy %s shutdown: %w", s.name, err)
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sub := extractSubdomain(r.Host)
	if sub == "" {
		http.Error(w, "missing subdomain", http.StatusBadRequest)
		return
	}

	p, ok := s.mgr.GetBySubdomain(sub)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if p.Gateway != s.name {
		http.NotFound(w, r)
		return
	}
	if p.Status() != manager.StatusRunning {
		http.Error(w, "process stopped", http.StatusBadGateway)
		return
	}

	target := &url.URL{
		Scheme: "http",
		Host:   "127.0.0.1:" + strconv.Itoa(p.Port),
	}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.ErrorHandler = func(rw http.ResponseWriter, _ *http.Request, err error) {
		s.logger.Warn("proxy error", "subdomain", sub, "port", p.Port, "err", err)
		http.Error(rw, fmt.Sprintf("upstream error: %v", err), http.StatusBadGateway)
	}
	rp.ServeHTTP(w, r)
}

// extractSubdomain returns the first DNS label of Host, or "" if Host has fewer
// than two labels (i.e. no subdomain to extract).
func extractSubdomain(host string) string {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	host = strings.TrimSuffix(host, ".")
	if host == "" {
		return ""
	}
	i := strings.IndexByte(host, '.')
	if i <= 0 {
		return ""
	}
	return strings.ToLower(host[:i])
}
