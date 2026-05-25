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
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err.Error())
		os.Exit(1)
	}

	mgr := manager.New(cfg.LogBufferSize, cfg.StoppedTTL)
	mcpServer := mcp.New(mgr, cfg.Gateways, logger.With("component", "mcp"))

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

func gatewayNames(gws []config.Gateway) []string {
	out := make([]string, len(gws))
	for i, g := range gws {
		out[i] = g.Name
	}
	return out
}
