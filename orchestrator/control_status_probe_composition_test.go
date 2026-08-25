package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestControlStatusProbeSweepForwardsExactPlanAndCompletionEnvelope(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload map[string]any
		plan    map[string]any
		result  map[string]any
	}{
		{
			name: "plan",
			payload: map[string]any{"request": map[string]any{
				"request_id": "probe-1", "observed_at": "2026-07-25T18:00:00Z", "limit": int64(10),
			}},
			plan: map[string]any{"effects": []any{map[string]any{
				"version":         "sessions.control/v1",
				"effect_id": "control/status-probe/probe-1/api",
				"idempotency_key": "control/status-probe/probe-1/api",
				"kind":            "host.http.probe.v1",
				"request_id":      "probe-1",
				"component_slug":  "api",
				"url":             "https://status.example.test/api",
				"observed_at":     "2026-07-25T18:00:00Z",
			}}},
			result: map[string]any{"result": map[string]any{
				"request_id": "probe-1", "probed": int64(1), "rolled_up": int64(0), "observations": []any{},
			}},
		},
		{
			name: "completion",
			payload: map[string]any{
				"request":  map[string]any{"request_id": "probe-1", "observed_at": "2026-07-25T18:00:00Z", "limit": int64(10)},
				"effects":  []any{map[string]any{"effect_id": "control/status-probe/probe-1/api", "kind": "host.http.probe.v1"}},
				"receipts": []any{map[string]any{"effect_id": "control/status-probe/probe-1/api", "status": "operational"}},
			},
			plan: map[string]any{"result": map[string]any{
				"request_id": "probe-1", "probed": int64(1), "rolled_up": int64(0), "observations": []any{},
			}},
		},
	} {
			t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			controlCalls := 0
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				switch target + "/" + function {
				case "control/control.status-components.probe-sweep.v1":
					controlCalls++
					if controlCalls == 1 {
						return fleetWire(test.plan)
					}
					return fleetWire(test.result)
				case "effects/effects.execute.v1":
					var intent map[string]any
					if err := msgpack.Unmarshal(payload, &intent); err != nil {
						return nil, err
					}
					if intent["version"] != "pulp.effect.v1" || intent["id"] != "control/status-probe/probe-1/api" ||
						intent["idempotency_key"] != "control/status-probe/probe-1/api" || intent["kind"] != "host.http.probe.v1" {
						return nil, fmt.Errorf("unexpected status probe effect %#v", intent)
					}
					return fleetWire(map[string]any{
						"version": "pulp.effect.v1", "intent_id": intent["id"], "kind": intent["kind"],
						"idempotency_key": intent["idempotency_key"], "status": "completed",
						"result": map[string]any{"status": "operational", "http_status": int64(200)},
					})
				default:
					return nil, fmt.Errorf("unexpected status probe call %s/%s", target, function)
				}
			})
			defer runtime.Close()
			if _, err := runtime.Dispatch(workflow.DispatchRequest{
				Event: "control.status-components.probe-sweep.v1", Payload: test.payload,
			}); err != nil {
				t.Fatalf("Dispatch status probe %s: %v", test.name, err)
			}
			if test.name == "plan" {
				assertFleetCalls(t, calls,
					"control/control.status-components.probe-sweep.v1",
					"effects/effects.execute.v1",
					"control/control.status-components.probe-sweep.v1",
				)
			} else {
				assertFleetCalls(t, calls, "control/control.status-components.probe-sweep.v1")
			}
		})
	}
}
