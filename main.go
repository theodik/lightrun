package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/theodik/lightrun/config"
	"github.com/theodik/lightrun/manager"
	"github.com/theodik/lightrun/mcp"
	"github.com/theodik/lightrun/proxy"
	"github.com/theodik/lightrun/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err.Error())
		os.Exit(1)
	}

	mgr := manager.New(cfg.LogBufferSize, cfg.StoppedTTL, cfg.BinaryBaseDir)
	if cfg.StateFile != "" && cfg.StateFile != "off" {
		mgr.SetStore(manager.NewFileStore(cfg.StateFile, logger.With("component", "store")))
		restoreServices(mgr, logger)
	} else {
		logger.Info("service persistence disabled")
	}
	mcpServer := mcp.New(mgr, cfg.Gateways, logger.With("component", "mcp"))
	webHandler := web.New(mgr, cfg.Gateways, logger.With("component", "web"))
	mcpServer.Handle("/", webHandler)

	// signalCtx cancels on SIGINT/SIGTERM. errgroup derives a child ctx that
	// also cancels if any g.Go func returns a non-nil error — so signal or
	// failure both trigger graceful shutdown of every server.
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	g, runCtx := errgroup.WithContext(signalCtx)

	g.Go(func() error {
		return mcpServer.Run(runCtx, ":"+strconv.Itoa(cfg.MCPPort))
	})

	for _, gw := range cfg.Gateways {
		p := proxy.New(mgr, gw.Name, gw.Port, logger.With("component", "proxy", "gateway", gw.Name))
		g.Go(func() error {
			return p.Run(runCtx)
		})
	}

	logger.Info("lightrun started",
		"mcp_port", cfg.MCPPort,
		"gateways", gatewayNames(cfg.Gateways),
	)

	err = g.Wait()
	switch {
	case err == nil, err == context.Canceled:
		logger.Info("shutdown initiated")
	default:
		logger.Error("server error", "err", err.Error())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mgr.StopAll(shutdownCtx)
	logger.Info("lightrun stopped")
}

// restoreServices loads the named command registry. Commands are not
// launched on boot; the dashboard and run_command tool start them explicitly.
func restoreServices(mgr *manager.Manager, logger *slog.Logger) {
	results, err := mgr.Restore()
	if err != nil {
		logger.Error("restore: could not read state file", "err", err.Error())
		return
	}
	if len(results) == 0 {
		return
	}
	for _, r := range results {
		logger.Info("restored command registration",
			"subdomain", r.Opts.Subdomain, "name", r.Opts.Name, "gateway", r.Opts.Gateway, "port", r.Opts.Port)
	}
	logger.Info("command registry restore complete", "registered", len(results))
}

func gatewayNames(gws []config.Gateway) []string {
	out := make([]string, len(gws))
	for i, g := range gws {
		out[i] = g.Name
	}
	return out
}
