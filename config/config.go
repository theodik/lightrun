package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Gateway describes one named proxy listener. Each gateway corresponds to a
// Traefik router with its own middleware chain — lightrun itself only routes
// processes to the matching listener and has no opinion on what middleware
// (auth, rate limit, IP allowlist, ...) sits in front.
type Gateway struct {
	Name        string
	Port        int
	URL         string // template with %s for subdomain
	Description string
}

type Config struct {
	MCPPort       int
	LogBufferSize int
	StoppedTTL    time.Duration // 0 disables the janitor — stopped processes are kept forever
	BinaryBaseDir string        // resolves the leading '~' in start-tool binary_path; default $HOME
	Gateways      []Gateway
}

func Load() (Config, error) {
	cfg := Config{
		MCPPort:       envInt("LIGHTRUN_MCP_PORT", 18082),
		LogBufferSize: envInt("LIGHTRUN_LOG_BUFFER_SIZE", 1000),
		StoppedTTL:    envDuration("LIGHTRUN_STOPPED_TTL", time.Hour),
		BinaryBaseDir: binaryBaseDir(),
	}
	gws, err := parseGateways(os.Environ())
	if err != nil {
		return Config{}, err
	}
	for _, g := range gws {
		if g.Port == cfg.MCPPort {
			return Config{}, fmt.Errorf("gateway %q port %d conflicts with MCP port (LIGHTRUN_MCP_PORT)", g.Name, g.Port)
		}
	}
	cfg.Gateways = gws
	return cfg, nil
}

// LIGHTRUN_GATEWAY_<NAME>_<FIELD> where NAME may contain '_' (becomes '-' in
// the lowercased gateway name) and FIELD is one of PORT, URL, DESCRIPTION.
var gatewayEnvRe = regexp.MustCompile(`^LIGHTRUN_GATEWAY_([A-Z0-9][A-Z0-9_]*?)_(PORT|URL|DESCRIPTION)$`)

var gatewayNameRe = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

func parseGateways(environ []string) ([]Gateway, error) {
	raw := make(map[string]map[string]string)
	for _, kv := range environ {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		key, val := kv[:eq], kv[eq+1:]
		m := gatewayEnvRe.FindStringSubmatch(key)
		if m == nil {
			continue
		}
		name := strings.ToLower(strings.ReplaceAll(m[1], "_", "-"))
		if raw[name] == nil {
			raw[name] = map[string]string{}
		}
		raw[name][m[2]] = val
	}

	if len(raw) == 0 {
		return nil, errors.New("no gateways configured; set at least one LIGHTRUN_GATEWAY_<NAME>_PORT and LIGHTRUN_GATEWAY_<NAME>_URL")
	}

	names := make([]string, 0, len(raw))
	for n := range raw {
		names = append(names, n)
	}
	sort.Strings(names)

	seenPorts := map[int]string{}
	gateways := make([]Gateway, 0, len(names))
	for _, n := range names {
		if !gatewayNameRe.MatchString(n) {
			return nil, fmt.Errorf("invalid gateway name %q (must match %s)", n, gatewayNameRe)
		}
		fields := raw[n]
		portStr, ok := fields["PORT"]
		if !ok {
			return nil, fmt.Errorf("gateway %q: missing PORT", n)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("gateway %q: invalid PORT %q", n, portStr)
		}
		if other, dup := seenPorts[port]; dup {
			return nil, fmt.Errorf("gateways %q and %q both use port %d", other, n, port)
		}
		seenPorts[port] = n

		url := fields["URL"]
		if url == "" {
			return nil, fmt.Errorf("gateway %q: missing URL", n)
		}
		if !strings.Contains(url, "%s") {
			return nil, fmt.Errorf("gateway %q: URL %q must contain %%s placeholder for subdomain", n, url)
		}

		gateways = append(gateways, Gateway{
			Name:        n,
			Port:        port,
			URL:         url,
			Description: fields["DESCRIPTION"],
		})
	}
	return gateways, nil
}

// binaryBaseDir is the directory that 'start' tool callers can reference via a
// leading '~' in binary_path. Falls back to the lightrun process's $HOME so the
// default mirrors shell convention; the workspace container shares the same
// HOME mount, so both sides see the same paths.
func binaryBaseDir() string {
	if v := os.Getenv("LIGHTRUN_BINARY_BASE_DIR"); v != "" {
		return v
	}
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return ""
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
