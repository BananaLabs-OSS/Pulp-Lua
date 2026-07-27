package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
)

func TestControlStatusProbeSweepForwardsExactPlanAndCompletionEnvelope(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload map[string]any
		result  map[string]any
	}{
		{
			name: "plan",
			payload: map[string]any{"request": map[string]any{
				"request_id": "probe-1", "observed_at": "2026-07-25T18:00:00Z", "limit": int64(10),
			}},
			result: map[string]any{"effects": []any{map[string]any{
				"effect_id": "control/status-probe/probe-1/api",
				"kind":      "host.http.probe.v1",
			}}},
		},
		{
			name: "completion",
			payload: map[string]any{
				"request":  map[string]any{"request_id": "probe-1", "observed_at": "2026-07-25T18:00:00Z", "limit": int64(10)},
				"effects":  []any{map[string]any{"effect_id": "control/status-probe/probe-1/api", "kind": "host.http.probe.v1"}},
				"receipts": []any{map[string]any{"effect_id": "control/status-probe/probe-1/api", "status": "operational"}},
			},
			result: map[string]any{"result": map[string]any{
				"request_id": "probe-1", "probed": int64(1), "rolled_up": int64(0), "observations": []any{},
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				if target != "control" || function != "control.status-components.probe-sweep.v1" {
					return nil, fmt.Errorf("unexpected status probe call %s/%s", target, function)
				}
				return fleetWire(test.result)
			})
			defer runtime.Close()
			if _, err := runtime.Dispatch(workflow.DispatchRequest{
				Event: "control.status-components.probe-sweep.v1", Payload: test.payload,
			}); err != nil {
				t.Fatalf("Dispatch status probe %s: %v", test.name, err)
			}
			assertFleetCalls(t, calls, "control/control.status-components.probe-sweep.v1")
		})
	}
}
