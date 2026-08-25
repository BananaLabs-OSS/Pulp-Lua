package orchestrator

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestSessionsPublicCatalogV1UsesGenericControlProjection(t *testing.T) {
	calls := 0
	runtime, err := New(Options{
		Script: evolutionLua(t),
		Caller: CallFunc(func(target, function string, payload []byte) ([]byte, error) {
			calls++
			if target != "control" || function != "control.v1.query" {
				t.Fatalf("unexpected Control call %s/%s", target, function)
			}
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["version"] != "sessions.control/v1" || request["kind"] != "projection" {
				t.Fatalf("projection request = %#v", request)
			}
			return msgpack.Marshal(map[string]any{
				"version": "sessions.control/v1",
				"games": []any{map[string]any{
					"id": "minecraft", "name": "Minecraft", "enabled": true, "visible": true,
					"primary_template": "vanilla", "icon": "minecraft", "tagline": "Build together",
					"description": "A shared world", "tags": []any{}, "max_players": int64(20),
				}},
				"tiers": []any{map[string]any{
					"id": "starter", "game_id": "minecraft", "enabled": true, "sort_order": int64(1),
					"label": "Starter", "price_cents": int64(500), "duration": "30d",
					"max_cpu": int64(2), "max_ram_mb": int64(2048),
				}},
				"visibility": []any{},
			})
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{Event: "evolution.sessions.catalog.get.v1"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("Control calls = %d, want 1", calls)
	}
	packed, ok := result.Value.(string)
	if !ok {
		t.Fatalf("catalog response = %#v", result.Value)
	}
	var response struct {
		Status uint32            `msgpack:"status"`
		Headers map[string]string `msgpack:"headers"`
		Body   string             `msgpack:"body"`
	}
	if err := msgpack.Unmarshal([]byte(packed), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != 200 || response.Headers["Content-Type"] != "application/json" || response.Body == "" {
		t.Fatalf("catalog response = %#v", response)
	}
}
