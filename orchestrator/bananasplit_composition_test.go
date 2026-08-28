package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestBananasplitLuaComposesGenericHTTPForRouteAssignment(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "Bananasplit", "application", "bananasplit.lua"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	caller := CallFunc(func(target, provider string, payload []byte) ([]byte, error) {
		calls = append(calls, target+"/"+provider)
		switch provider {
		case "engine.http-json.v1.request":
			return msgpack.Marshal(map[string]any{
				"status": int64(200),
				"value": []any{map[string]any{
					"id": "lobby-1", "host": "198.51.100.8", "port": int64(5520),
					"players": int64(2), "maxPlayers": int64(20),
				}},
			})
		case "coordination.v1.directory.put":
			return msgpack.Marshal(map[string]any{"ok": true})
		case "coordination.v1.queue.join":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				return nil, err
			}
			member := request["member"].(map[string]any)
			if request["queue"] != "duel" || member["participant_id"] != "player-1" || member["origin"] != "lobby-1" {
				t.Fatalf("generic queue request = %#v", request)
			}
			return msgpack.Marshal(map[string]any{"status": "queued", "queue": "duel", "position": int64(1)})
		default:
			t.Fatalf("unexpected call %s/%s", target, provider)
			return nil, nil
		}
	})
	runtime, err := New(Options{
		Script: string(script), Caller: caller,
		Config: map[string]any{
			"bananagine_url":   "http://bananagine:3000",
			"bananagine_token": "service-token",
			"peel_url":         "",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{Event: "bananasplit.http.route-request.v1", Payload: map[string]any{
		"id": "route-1", "player_ip": "203.0.113.9",
	}})
	if err != nil {
		t.Fatal(err)
	}
	value := result.Value.(map[string]any)
	if value["backend"] != "198.51.100.8:5520" || value["server_id"] != "lobby-1" {
		t.Fatalf("route result = %#v", value)
	}
	want := []string{
		"http-json/engine.http-json.v1.request",
		"coordination-state/coordination.v1.directory.put",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
	for index := range want {
		if calls[index] != want[index] {
			t.Fatalf("calls = %#v, want %#v", calls, want)
		}
	}

	joined, err := runtime.Dispatch(DispatchRequest{Event: "bananasplit.http.queue.join.v1", Payload: map[string]any{
		"id": "join-1", "mode": "duel", "uuid": "player-1", "lobby_server": "lobby-1", "joined_at": "2026-08-28T12:00:00Z",
	}})
	if err != nil {
		t.Fatal(err)
	}
	joinValue := joined.Value.(map[string]any)
	if joinValue["mode"] != "duel" || joinValue["position"] != int64(1) {
		t.Fatalf("queue join result = %#v", joinValue)
	}
	if calls[len(calls)-1] != "coordination-state/coordination.v1.queue.join" {
		t.Fatalf("queue call = %q", calls[len(calls)-1])
	}
}
