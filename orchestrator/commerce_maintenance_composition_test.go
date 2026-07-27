package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCommerceMaintenanceEventMatrixUsesExactOwnerProviders(t *testing.T) {
	tests := []struct {
		event    string
		provider string
		command  map[string]any
	}{
		{
			"commerce.workflow.maintenance.refund.reconcile.v1",
			"commerce.maintenance.refund.reconcile.v1",
			map[string]any{"idempotency_key": "refund-1", "order_id": "order-1", "now_unix": int64(1_750_000_000), "min_age_seconds": int64(600)},
		},
		{
			"commerce.workflow.maintenance.coupon.reconcile.v1",
			"commerce.maintenance.coupon.reconcile.v1",
			map[string]any{"idempotency_key": "coupon-1", "now_unix": int64(1_750_000_000), "stale_after_seconds": int64(600), "limit": int64(100)},
		},
		{
			"commerce.workflow.maintenance.prospect.retain.v1",
			"commerce.maintenance.prospect.retain.v1",
			map[string]any{"idempotency_key": "prospect-1", "now_unix": int64(1_750_000_000), "retention_seconds": int64(86400), "limit": int64(100)},
		},
	}
	for _, test := range tests {
		t.Run(test.event, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				if target != "commerce" || function != test.provider {
					return nil, fmt.Errorf("unexpected maintenance call %s/%s", target, function)
				}
				var command map[string]any
				if err := msgpack.Unmarshal(payload, &command); err != nil {
					t.Fatal(err)
				}
				if command["idempotency_key"] == nil {
					t.Fatalf("maintenance command lacks idempotency key: %#v", command)
				}
				return fleetWire(map[string]any{"ok": true, "value": map[string]any{}})
			})
			defer runtime.Close()
			if _, err := runtime.Dispatch(workflow.DispatchRequest{
				Event: test.event, Payload: map[string]any{"command": test.command},
			}); err != nil {
				t.Fatalf("Dispatch(%s): %v", test.event, err)
			}
			assertFleetCalls(t, calls, "commerce/"+test.provider)
		})
	}
}
