package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
)

func TestCommerceAdminReadsUseFinalLegacyProviders(t *testing.T) {
	tests := []struct {
		operation string
		provider  string
		value     any
	}{
		{"promotion_get", "commerce.admin.promotion.current.v1", map[string]any{"active": false}},
		{"promotion_list", "commerce.admin.promotion.list-legacy.v1", []any{}},
		{"coupon_list", "commerce.admin.coupon.list-legacy.v1", []any{}},
		{"coupon_request_list", "commerce.admin.coupon-request.list-legacy.v1", []any{}},
		{"allowlist_list", "commerce.admin.coupon-allowlist.list-legacy.v1", []any{}},
		{"gift_list", "commerce.admin.gifts.list.v1", map[string]any{"gifts": []any{}, "page": int64(1), "page_size": int64(20), "total": int64(0)}},
		{"pending_review_list", "commerce.order.pending-review.list.v1", map[string]any{"orders": []any{}}},
		{"founding_member_list", "commerce.founding-member.list.v1", []any{}},
	}
	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				switch target + "/" + function {
				case "commerce/" + test.provider:
					return fleetWire(map[string]any{"ok": true, "value": test.value})
				case "commerce/commerce.admin.http.project.v1":
					return fleetWire(map[string]any{"ok": true, "value": map[string]any{
						"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": "[]",
					}})
				default:
					return nil, fmt.Errorf("unexpected admin read call %s/%s", target, function)
				}
			})
			defer runtime.Close()
			if _, err := runtime.Dispatch(workflow.DispatchRequest{
				Event: "evolution.sessions.commerce.admin.read-http.v1",
				Payload: map[string]any{
					"actor":     map[string]any{"id": "admin-1", "is_admin": true},
					"operation": test.operation, "limit": int64(100),
				},
			}); err != nil {
				t.Fatalf("Dispatch(%s): %v", test.operation, err)
			}
			assertFleetCalls(t, calls,
				"commerce/"+test.provider,
				"commerce/commerce.admin.http.project.v1",
			)
		})
	}
}
