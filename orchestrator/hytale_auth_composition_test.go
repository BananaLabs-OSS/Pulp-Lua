package orchestrator

import (
	"os"
	"testing"
)

func TestHytaleAuthLuaCompositionLoads(t *testing.T) {
	script, err := os.ReadFile("../../Hytale-Auth/application/hytale-auth.lua")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(Options{Script: string(script), Config: map[string]any{
		"client_id": "hytale-server", "scope": "openid offline auth:server",
		"device_url": "https://example.invalid/device", "token_url": "https://example.invalid/token",
		"profiles_url": "https://example.invalid/profiles", "session_url": "https://example.invalid/session",
	}})
	if err != nil {
		t.Fatalf("load Hytale Auth Lua composition: %v", err)
	}
	defer runtime.Close()
}
