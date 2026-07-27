package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFleetAdminExactEventMatrix(t *testing.T) {
	tests := []struct {
		name     string
		event    string
		provider string
		request  map[string]any
		control  bool
		admin    bool
	}{
		{"reconcile", "fleet.workflow.admin.reconcile.http.v1", "fleet.v1.admin.reconcile.http", map[string]any{"request_id": "reconcile-1"}, false, true},
		{"spark", "fleet.workflow.admin.spark.http.v1", "fleet.v1.admin.spark.http", map[string]any{"request_id": "spark-1", "server_id": "server-1"}, false, true},
		{"connections", "fleet.workflow.admin.connections.http.v1", "fleet.v1.admin.connections.http", map[string]any{"server_id": "server-1"}, false, true},
		{"flush-destroy-start", "fleet.workflow.admin.flush-destroy.start.v1", "fleet.v1.admin.flush-destroy.start", map[string]any{"id": "flush-destroy-1", "server_id": "server-1", "reason": "admin"}, false, true},
		{"update-all", "fleet.workflow.admin.update-all.http.v1", "fleet.v1.admin.update-all.http", map[string]any{"request_id": "update-1"}, true, true},
		{"version-deploy", "fleet.workflow.admin.version.deploy.http.v1", "fleet.v1.admin.version.deploy.http", map[string]any{"request_id": "deploy-1", "game_id": "minecraft"}, true, true},
		{"connection-report", "fleet.workflow.connection.report.v1", "fleet.v1.command.connection.report", map[string]any{"connection_id": "connection-1", "server_id": "server-1", "player_name": "Player", "event": "connect", "occurred_at": "2026-07-25T18:00:00Z"}, false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				switch target + "/" + function {
				case "control/control.v1.query":
					projection := fleetControlProjection()
					projection["games"] = []any{map[string]any{"id": "minecraft"}}
					return fleetWire(projection)
				case "fleet/" + test.provider:
					var request map[string]any
					if err := msgpack.Unmarshal(payload, &request); err != nil {
						t.Fatal(err)
					}
					return fleetWire(map[string]any{"status": int64(200), "body": []byte(`{}`)})
				default:
					return nil, fmt.Errorf("unexpected admin call %s/%s", target, function)
				}
			})
			defer runtime.Close()
			payload := map[string]any{"request": test.request}
			if test.name == "connection-report" {
				innerRequest, err := msgpack.Marshal(test.request)
				if err != nil {
					t.Fatalf("marshal connection report: %v", err)
				}
				requestMsgpack, err := msgpack.Marshal(map[string]any{
					"event":              test.event,
					"host_verified":      true,
					"subject_id":         "server-1",
					"dispatched_at_unix": int64(1_753_469_800),
					"expires_at_unix":    int64(1_753_470_100),
					"request_id":         "connection-report-1",
					"idempotency_key":    "connection-report-1",
					"request":            innerRequest,
				})
				if err != nil {
					t.Fatalf("marshal trusted connection report envelope: %v", err)
				}
				payload = map[string]any{"request_msgpack": requestMsgpack}
			}
			if test.admin {
				payload["actor"] = map[string]any{"id": "admin-1", "is_admin": true}
			}
			if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: test.event, Payload: payload}); err != nil {
				t.Fatalf("Dispatch(%s): %v", test.event, err)
			}
			want := []string{}
			if test.control {
				want = append(want, "control/control.v1.query")
			}
			want = append(want, "fleet/"+test.provider)
			assertFleetCalls(t, calls, want...)
		})
	}
}

func TestFleetAdminVersionsRejectsCallerFabricatedFacts(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		return nil, fmt.Errorf("forbidden owner call %s/%s", target, function)
	})
	defer runtime.Close()
	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "fleet.workflow.admin.versions.http.v1",
		Payload: map[string]any{
			"actor": map[string]any{"id": "admin-1", "is_admin": true},
			"request": map[string]any{
				"versions": []any{map[string]any{"game": "minecraft", "image_tag": "caller-value"}},
			},
		},
	})
	if err == nil {
		t.Fatal("admin versions accepted caller-fabricated transient facts")
	}
	if len(calls) != 0 {
		t.Fatalf("admin versions called owners before exact Control facts existed: %#v", calls)
	}
}
