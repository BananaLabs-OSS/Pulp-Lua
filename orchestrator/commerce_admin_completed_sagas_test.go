package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
)

func TestCommerceAdminCompletedCouponSagaExecutesAndAppliesExactReceipt(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.coupon.delete-saga.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"id": "saga-1", "status": "prepared",
				"effects": []any{map[string]any{
					"id": "effect-1", "kind": "stripe.coupon.delete",
					"idempotency_key": "coupon-delete-1", "payload": []byte{0x81, 0xa2, 'i', 'd', 0xa8, 'c', 'o', 'u', 'p', 'o', 'n', '-', '1'},
				}},
			}})
		case "effects/effects.execute.v1":
			return fleetWire(map[string]any{
				"intent_id": "effect-1", "kind": "stripe.coupon.delete",
				"idempotency_key": "coupon-delete-1", "status": "completed", "result": []byte{0x80},
			})
		case "commerce/commerce.admin.coupon-saga.effect.apply.v1":
			assertFleetField(t, payload, "saga_id", "saga-1")
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"id": "saga-1", "status": "completed",
				"legacy_response": map[string]any{"deleted": true},
			}})
		case "commerce/commerce.admin.http.project.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": `{"deleted":true}`,
			}})
		default:
			return nil, fmt.Errorf("unexpected completed coupon saga call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.coupon-delete.completed.v1",
		Payload: map[string]any{
			"request_id": "request-1",
			"actor":      map[string]any{"id": "admin-1", "is_admin": true},
			"command":     map[string]any{"saga_id": "saga-1", "coupon_id": "coupon-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.coupon.delete-saga.v1",
		"effects/effects.execute.v1",
		"commerce/commerce.admin.coupon-saga.effect.apply.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}

func TestCommerceAdminCompletedDirectMutationProjectsOwnerResult(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.promotion.save.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{"saved": true}})
		case "commerce/commerce.admin.http.project.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": `{"saved":true}`,
			}})
		default:
			return nil, fmt.Errorf("unexpected completed direct mutation call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	if _, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.promotion-save.completed.v1",
		Payload: map[string]any{
			"request_id": "request-2",
			"actor":      map[string]any{"id": "admin-1", "is_admin": true},
			"command":     map[string]any{"code": "SAVE20"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.promotion.save.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}
