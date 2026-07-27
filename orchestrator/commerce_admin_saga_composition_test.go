package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
)

func TestCommerceAdminCouponBatchAppliesOwnerReceiptsBeforeProjection(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.coupon.batch-mint.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"codes": []any{"FOUNDING20"}, "count": int64(1), "stripe_failures": int64(0),
				"effects": []any{map[string]any{"id": "coupon-effect-1"}},
			}})
		case "commerce/commerce.admin.coupon.stripe-apply.v1":
			assertFleetField(t, payload, "coupon_id", "coupon-1")
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{}})
		case "commerce/commerce.admin.http.project.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": `{"codes":["FOUNDING20"],"count":1,"stripe_failures":0}`,
			}})
		default:
			return nil, fmt.Errorf("unexpected coupon batch call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.coupon-batch.v1",
		Payload: map[string]any{
			"request_id": "request-1",
			"actor":      map[string]any{"id": "admin-1", "is_admin": true},
			"command": map[string]any{"coupons": []any{map[string]any{
				"id": "coupon-1", "code": "FOUNDING20", "discount_cents": int64(2000),
			}}},
			"coupon_receipts": []any{map[string]any{
				"coupon_id": "coupon-1",
				"receipt":   map[string]any{"intent_id": "coupon-effect-1", "status": "completed"},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.coupon.batch-mint.v1",
		"commerce/commerce.admin.coupon.stripe-apply.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}

func TestCommercePendingReviewApprovalIsTwoPhase(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.order.pending-review.approve.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"review_id": "review-1", "effects": []any{map[string]any{"id": "payment-effect-1"}},
			}})
		case "commerce/commerce.order.pending-review.approval.apply.v1":
			assertFleetField(t, payload, "effect_id", "payment-effect-1")
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"legacy_response": map[string]any{"ok": true, "order_id": "order-1", "stripe_session_id": "pi-1"},
			}})
		case "commerce/commerce.admin.http.project.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": `{"ok":true}`,
			}})
		default:
			return nil, fmt.Errorf("unexpected pending-review call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.pending-review.approve.v1",
		Payload: map[string]any{
			"request_id": "request-1",
			"actor":      map[string]any{"id": "admin-1", "is_admin": true},
			"command":     map[string]any{"review_id": "review-1", "order_id": "order-1"},
			"effect_id":   "payment-effect-1",
			"receipt":     map[string]any{"intent_id": "payment-effect-1", "status": "completed"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.order.pending-review.approve.v1",
		"commerce/commerce.order.pending-review.approval.apply.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}

func TestCommerceFoundingIssueReturnsEachDependentEffectPhase(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.founding-member.issue.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"issued": int64(1), "results": []any{map[string]any{"email": "founder@example.com", "status": "issued"}},
			}})
		case "commerce/commerce.founding-member.effect.apply.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"effects":  []any{map[string]any{"id": "promotion-effect-1"}},
				"receipts": []any{map[string]any{"intent_id": "promotion-effect-1", "status": "pending"}},
			}})
		default:
			return nil, fmt.Errorf("unexpected founding issue call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.founding.issue.v1",
		Payload: map[string]any{
			"request_id": "request-1",
			"actor":      map[string]any{"id": "admin-1", "is_admin": true},
			"command": map[string]any{
				"emails": []any{"founder@example.com"}, "codes": []any{"FOUNDING20"}, "discount_cents": int64(2000),
			},
			"receipts": []any{map[string]any{"intent_id": "coupon-effect-1", "status": "completed"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.founding-member.issue.v1",
		"commerce/commerce.founding-member.effect.apply.v1",
	)
}

func TestCommerceAdminGiftCreationRequiresTrustedSeeds(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.gift.create.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{"id": "gift-1", "gift_token": "secret-token"}})
		case "commerce/commerce.admin.http.project.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": `{"id":"gift-1"}`,
			}})
		default:
			return nil, fmt.Errorf("unexpected gift creation call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.gift.create.v1",
		Payload: map[string]any{
			"request_id": "request-1",
			"actor":      map[string]any{"id": "admin-1", "is_admin": true},
			"gift":       map[string]any{"order_id": "order-1", "gift_token": "secret-token", "server_type": "paper"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.gift.create.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}
