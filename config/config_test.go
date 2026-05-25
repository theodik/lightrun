package config

import (
	"strings"
	"testing"
)

func TestParseGateways_Happy(t *testing.T) {
	environ := []string{
		"LIGHTRUN_GATEWAY_AUTH_PORT=18080",
		"LIGHTRUN_GATEWAY_AUTH_URL=https://%s.app.example.com",
		"LIGHTRUN_GATEWAY_AUTH_DESCRIPTION=Behind OAuth",
		"LIGHTRUN_GATEWAY_PUBLIC_PORT=18081",
		"LIGHTRUN_GATEWAY_PUBLIC_URL=https://%s.pub.example.com",
		"UNRELATED=ignored",
		"LIGHTRUN_GATEWAY_=ignored",
	}
	gws, err := parseGateways(environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(gws) != 2 {
		t.Fatalf("got %d gateways, want 2", len(gws))
	}
	// Sorted alphabetically by name.
	if gws[0].Name != "auth" || gws[1].Name != "public" {
		t.Errorf("names = %s, %s; want auth, public", gws[0].Name, gws[1].Name)
	}
	if gws[0].Port != 18080 || gws[1].Port != 18081 {
		t.Errorf("ports = %d, %d", gws[0].Port, gws[1].Port)
	}
	if gws[0].Description != "Behind OAuth" || gws[1].Description != "" {
		t.Errorf("descriptions = %q, %q", gws[0].Description, gws[1].Description)
	}
}

func TestParseGateways_UnderscoreInName(t *testing.T) {
	environ := []string{
		"LIGHTRUN_GATEWAY_ADMIN_ONLY_PORT=18090",
		"LIGHTRUN_GATEWAY_ADMIN_ONLY_URL=https://%s.admin.example.com",
	}
	gws, err := parseGateways(environ)
	if err != nil {
		t.Fatal(err)
	}
	if len(gws) != 1 {
		t.Fatalf("got %d gateways, want 1", len(gws))
	}
	if gws[0].Name != "admin-only" {
		t.Errorf("name = %q, want admin-only", gws[0].Name)
	}
}

func TestLoad_MCPPortGatewayConflict(t *testing.T) {
	t.Setenv("LIGHTRUN_MCP_PORT", "18080")
	t.Setenv("LIGHTRUN_GATEWAY_X_PORT", "18080")
	t.Setenv("LIGHTRUN_GATEWAY_X_URL", "https://%s.example.com")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "conflicts with MCP port") {
		t.Errorf("got %v, want MCP-port conflict error", err)
	}
}

func TestParseGateways_Errors(t *testing.T) {
	cases := []struct {
		name    string
		environ []string
		wantSub string // substring expected in the error message
	}{
		{
			"no gateways",
			[]string{"UNRELATED=x"},
			"no gateways configured",
		},
		{
			"missing port",
			[]string{"LIGHTRUN_GATEWAY_X_URL=https://%s.example.com"},
			"missing PORT",
		},
		{
			"missing url",
			[]string{"LIGHTRUN_GATEWAY_X_PORT=18080"},
			"missing URL",
		},
		{
			"url without %s",
			[]string{
				"LIGHTRUN_GATEWAY_X_PORT=18080",
				"LIGHTRUN_GATEWAY_X_URL=https://example.com",
			},
			"%s placeholder",
		},
		{
			"non-numeric port",
			[]string{
				"LIGHTRUN_GATEWAY_X_PORT=abc",
				"LIGHTRUN_GATEWAY_X_URL=https://%s.example.com",
			},
			"invalid PORT",
		},
		{
			"port out of range",
			[]string{
				"LIGHTRUN_GATEWAY_X_PORT=99999",
				"LIGHTRUN_GATEWAY_X_URL=https://%s.example.com",
			},
			"invalid PORT",
		},
		{
			"duplicate port",
			[]string{
				"LIGHTRUN_GATEWAY_A_PORT=18080",
				"LIGHTRUN_GATEWAY_A_URL=https://%s.a.com",
				"LIGHTRUN_GATEWAY_B_PORT=18080",
				"LIGHTRUN_GATEWAY_B_URL=https://%s.b.com",
			},
			"both use port",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseGateways(c.environ)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), c.wantSub)
			}
		})
	}
}
