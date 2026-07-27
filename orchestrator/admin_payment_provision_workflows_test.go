package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func decodeAdminPaymentPayload(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := msgpack.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func adminPaymentProjection() ([]byte, error) {
	return fleetWire(map[string]any{"ok": true, "value": map[string]any{
		"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": []byte(`{}`),
	}})
}

func TestAdminPaymentPaidApprovalProjectsTerminalCommerceResult(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.pending-review.paid.approve.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["idempotency_key"] != "paid-key-1" || request["actor_id"] != "admin-1" {
				t.Fatalf("paid approval command = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"order": map[string]any{"id": "order-1", "status": "paid", "payment_status": "succeeded", "stripe_payment_id": "pi_paid"},
				"fact":  map[string]any{"order_id": "order-1"}, "receipt": map[string]any{"id": "receipt-1"},
				"legacy_response": map[string]any{"ok": true},
			}})
		case "commerce/commerce.admin.payment-provision.http.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["operation"] != "paid_pending_review_approve" || request["paid_approval"] == nil {
				t.Fatalf("paid terminal projection = %#v", request)
			}
			return adminPaymentProjection()
		default:
			return nil, fmt.Errorf("unexpected paid-approval call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.admin.pending-review.paid-approve.final-http.v1", Payload: map[string]any{
		"request_id": "paid-request-1", "actor": map[string]any{"id": "admin-1", "is_admin": true},
		"command": map[string]any{"idempotency_key": "paid-key-1", "order_id": "order-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.pending-review.paid.approve.v1",
		"commerce/commerce.admin.payment-provision.http.project.v1",
	)
}

func TestAdminPendingReviewApprovalExecutesAndAppliesExactPaymentReceipt(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.order.pending-review.approve.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["idempotency_key"] != "review-key-1" || request["review_id"] != "review-1" ||
				request["order_id"] != "order-1" || request["actor_id"] != "admin-1" {
				t.Fatalf("pending-review approval command = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"review_id": "review-1", "order_id": "order-1", "status": "payment_intent_pending",
				"effects": []any{map[string]any{
					"id": "review:review-1:stripe", "kind": "pulp.effect.stripe.payment-intent.create.v1",
					"idempotency_key": "review:review-1:payment-intent", "payload": []byte{0x80},
				}},
			}})
		case "effects/effects.execute.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["id"] != "review:review-1:stripe" ||
				request["kind"] != "pulp.effect.stripe.payment-intent.create.v1" ||
				request["idempotency_key"] != "review:review-1:payment-intent" {
				t.Fatalf("pending-review executor request = %#v", request)
			}
			return fleetWire(map[string]any{
				"intent_id": "review:review-1:stripe", "kind": "pulp.effect.stripe.payment-intent.create.v1",
				"idempotency_key": "review:review-1:payment-intent", "status": "completed",
				"result": []byte{0x83, 0xa2, 'i', 'd', 0xa6, 'p', 'i', '_', '1', '2', '3', 0xad, 'c', 'l', 'i', 'e', 'n', 't', '_', 's', 'e', 'c', 'r', 'e', 't', 0xa9, 'p', 'i', '_', 's', 'e', 'c', 'r', 'e', 't', 0xa6, 's', 't', 'a', 't', 'u', 's', 0xb7, 'r', 'e', 'q', 'u', 'i', 'r', 'e', 's', '_', 'p', 'a', 'y', 'm', 'e', 'n', 't', '_', 'm', 'e', 't', 'h', 'o', 'd'},
			})
		case "commerce/commerce.order.pending-review.approval.apply.v1":
			request := decodeAdminPaymentPayload(t, payload)
			receipt := request["receipt"].(map[string]any)
			if request["review_id"] != "review-1" || request["effect_id"] != "review:review-1:stripe" ||
				request["idempotency_key"] != "review-request-1:pending-review-receipt:review:review-1:stripe" ||
				receipt["intent_id"] != "review:review-1:stripe" {
				t.Fatalf("pending-review receipt apply = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"legacy_response": map[string]any{"ok": true, "order_id": "order-1", "stripe_session_id": "pi_123"},
			}})
		case "commerce/commerce.admin.http.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			result := request["result"].(map[string]any)
			value := result["value"].(map[string]any)
			if request["operation"] != "order_review_approve" || value["stripe_session_id"] != "pi_123" {
				t.Fatalf("pending-review HTTP projection = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"},
				"body": []byte(`{"ok":true,"order_id":"order-1","stripe_session_id":"pi_123"}`),
			}})
		default:
			return nil, fmt.Errorf("unexpected pending-review call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.admin.pending-review.approve.final-http.v1", Payload: map[string]any{
		"request_id": "review-request-1", "actor": map[string]any{"id": "admin-1", "is_admin": true},
		"command": map[string]any{"idempotency_key": "review-key-1", "review_id": "review-1", "order_id": "order-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.order.pending-review.approve.v1", "effects/effects.execute.v1",
		"commerce/commerce.order.pending-review.approval.apply.v1", "commerce/commerce.admin.http.project.v1",
	)
}

func TestAdminPaymentRefundSettlesExactReceiptBeforeFleetRequestAndProjection(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.refund.prepare.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"workflow": map[string]any{"saga_id": "refund-saga-1"},
				"effects":  []any{map[string]any{"id": "stripe-refund-1", "kind": "stripe.refund.create", "idempotency_key": "stripe-refund-key-1", "payload": []byte{0x81, 0xa2, 'o', 'k', 0xc3}}},
			}})
		case "effects/effects.execute.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["id"] != "stripe-refund-1" || request["kind"] != "stripe.refund.create" || request["idempotency_key"] != "stripe-refund-key-1" {
				t.Fatalf("refund executor request = %#v", request)
			}
			return fleetWire(map[string]any{"intent_id": "stripe-refund-1", "kind": "stripe.refund.create", "idempotency_key": "stripe-refund-key-1", "status": "completed", "result": []byte{0x80}})
		case "commerce/commerce.admin.refund.receipt.apply.v1":
			request := decodeAdminPaymentPayload(t, payload)
			receipt := request["receipt"].(map[string]any)
			if request["saga_id"] != "refund-saga-1" || request["effect_id"] != "stripe-refund-1" || receipt["intent_id"] != "stripe-refund-1" {
				t.Fatalf("refund receipt application = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"workflow":        map[string]any{"status": "refunded"},
				"fact":            map[string]any{"fact_id": "commerce-refund-1", "revision": int64(1), "saga_id": "refund-saga-1", "flow": "server_refund", "order_id": "order-1", "server_id": "server-1", "amount_cents": int64(1200), "payment_intent": "pi_refund", "receipt": receipt, "order": map[string]any{"fact_id": "order-fact-1", "revision": int64(1), "order_id": "order-1", "status": "refunded", "payment_status": "refunded", "stripe_payment_id": "pi_refund", "amount_cents": int64(1200)}},
				"legacy_response": map[string]any{"refunded": true},
			}})
		case "fleet/fleet.v1.admin.refund-destroy.plan":
			request := decodeAdminPaymentPayload(t, payload)
			if _, found := request["id"]; found {
				t.Fatalf("Lua created Fleet refund command identity: %#v", request)
			}
			return fleetWire(map[string]any{"id": "fleet-owned-refund-command", "commerce": request["commerce"]})
		case "fleet/fleet.v1.admin.refund-destroy.apply":
			request := decodeAdminPaymentPayload(t, payload)
			if request["id"] != "fleet-owned-refund-command" {
				t.Fatalf("Fleet refund apply did not receive Fleet-planned command: %#v", request)
			}
			return fleetWire(map[string]any{"destroy_requested": true})
		case "commerce/commerce.admin.payment-provision.http.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["operation"] != "refund" || request["refund"] == nil {
				t.Fatalf("refund terminal projection = %#v", request)
			}
			return adminPaymentProjection()
		default:
			return nil, fmt.Errorf("unexpected refund call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.admin.refund.final-http.v1", Payload: map[string]any{
		"request_id": "refund-request-1", "actor": map[string]any{"id": "admin-1", "is_admin": true},
		"command": map[string]any{"idempotency_key": "refund-key-1", "saga_id": "refund-saga-1", "flow": "server_refund", "order_id": "order-1", "server_id": "server-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.refund.prepare.v1", "effects/effects.execute.v1", "commerce/commerce.admin.refund.receipt.apply.v1",
		"fleet/fleet.v1.admin.refund-destroy.plan", "fleet/fleet.v1.admin.refund-destroy.apply",
		"commerce/commerce.admin.payment-provision.http.project.v1",
	)
}

func TestAdminPaymentOrderRefundWithoutServerSkipsFleetAfterCommerceSettlement(t *testing.T) {
	emptyServerID := ""
	for _, test := range []struct {
		name             string
		terminalServerID *string
	}{
		{name: "omitted_server_id"},
		{name: "empty_server_id", terminalServerID: &emptyServerID},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				switch target + "/" + function {
				case "commerce/commerce.admin.refund.prepare.v1":
					return fleetWire(map[string]any{"ok": true, "value": map[string]any{
						"workflow": map[string]any{"saga_id": "refund-saga-1"},
						"effects":  []any{map[string]any{"id": "stripe-refund-1", "kind": "stripe.refund.create", "idempotency_key": "stripe-refund-key-1", "payload": []byte{0x80}}},
					}})
				case "effects/effects.execute.v1":
					return fleetWire(map[string]any{"intent_id": "stripe-refund-1", "kind": "stripe.refund.create", "idempotency_key": "stripe-refund-key-1", "status": "completed", "result": []byte{0x80}})
				case "commerce/commerce.admin.refund.receipt.apply.v1":
					request := decodeAdminPaymentPayload(t, payload)
					fact := map[string]any{
						"fact_id": "commerce-refund-1", "revision": int64(1), "saga_id": "refund-saga-1",
						"flow": "order_refund_to_card", "order_id": "order-1", "amount_cents": int64(1200),
						"payment_intent": "pi_refund", "receipt": request["receipt"],
						"order": map[string]any{"fact_id": "order-fact-1", "revision": int64(1), "order_id": "order-1", "status": "refunded", "payment_status": "refunded", "stripe_payment_id": "pi_refund", "amount_cents": int64(1200)},
					}
					if test.terminalServerID != nil {
						fact["server_id"] = *test.terminalServerID
					}
					return fleetWire(map[string]any{"ok": true, "value": map[string]any{
						"workflow":        map[string]any{"status": "refunded"},
						"fact":            fact,
						"legacy_response": map[string]any{"refunded": true},
					}})
				case "commerce/commerce.admin.payment-provision.http.project.v1":
					request := decodeAdminPaymentPayload(t, payload)
					refund := request["refund"].(map[string]any)
					if request["operation"] != "refund" || refund["fact"] == nil {
						t.Fatalf("order refund terminal projection = %#v", request)
					}
					return adminPaymentProjection()
				default:
					return nil, fmt.Errorf("unexpected order refund call %s/%s", target, function)
				}
			})
			defer runtime.Close()

			command := map[string]any{
				"idempotency_key": "refund-key-1", "saga_id": "refund-saga-1",
				"flow": "order_refund_to_card", "order_id": "order-1",
			}
			if test.terminalServerID != nil {
				command["server_id"] = *test.terminalServerID
			}
			if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.admin.refund.final-http.v1", Payload: map[string]any{
				"request_id": "refund-request-1", "actor": map[string]any{"id": "admin-1", "is_admin": true},
				"command": command,
			}}); err != nil {
				t.Fatal(err)
			}
			assertFleetCalls(t, calls,
				"commerce/commerce.admin.refund.prepare.v1", "effects/effects.execute.v1",
				"commerce/commerce.admin.refund.receipt.apply.v1", "commerce/commerce.admin.payment-provision.http.project.v1",
			)
		})
	}
}

func TestAdminPaymentDirectProvisionFencesLeasedEffectAndNormalizesFleetEvidence(t *testing.T) {
	calls := []string{}
	commerceFact := map[string]any{"fact_id": "commerce-direct-1", "revision": int64(1), "order_id": "order-1", "server_id": "server-1", "template": "paper", "email": "player@example.test", "username": "player", "fulfillment_state": "granted", "order": map[string]any{"fact_id": "order-fact-1", "revision": int64(1), "order_id": "order-1", "status": "fulfilled", "payment_status": "succeeded", "amount_cents": int64(0)}}
	leaseIntent := map[string]any{"id": "provision-effect-1", "kind": "fleet.server.provision", "idempotency_key": "provision-key-1", "payload": []byte{0x81, 0xa2, 'i', 'd', 0xa8, 's', 'e', 'r', 'v', 'e', 'r', '-', '1'}}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.admin.provision.direct-order.prepare.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{"fact": commerceFact, "legacy_response": map[string]any{"server_id": "server-1", "order_id": "order-1", "status": "provisioning"}}})
		case "control/control.v1.query":
			return fleetWire(map[string]any{"version": "sessions.control/v1", "revision": int64(3),
				"visibility":        []any{map[string]any{"template": "paper", "enabled": true, "tier_id": "tier-1"}},
				"tiers":             []any{map[string]any{"id": "tier-1", "enabled": true, "max_cpu": float64(1), "max_ram_mb": int64(1024)}},
				"runtime_templates": []any{map[string]any{"template": "paper", "tier_id": "tier-1", "approved": true, "image": "registry.example/paper@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "approval_id": "approval-1", "policy_version": "policy-1"}},
			})
		case "fleet/fleet.v1.admin.direct-provision.plan":
			request := decodeAdminPaymentPayload(t, payload)
			if _, found := request["id"]; found {
				t.Fatalf("Lua created Fleet direct-provision command identity: %#v", request)
			}
			resource := request["resource"].(map[string]any)
			if resource["template"] != "paper" || fmt.Sprint(resource["memory_limit_mb"]) != "1024" || fmt.Sprint(resource["memory_weight"]) != "1" {
				t.Fatalf("Control resource fact = %#v", resource)
			}
			return fleetWire(map[string]any{"id": "fleet-owned-direct-command", "commerce": request["commerce"], "resource": resource, "image": request["image"]})
		case "fleet/fleet.v1.admin.direct-provision.start":
			request := decodeAdminPaymentPayload(t, payload)
			if request["id"] != "fleet-owned-direct-command" {
				t.Fatalf("Fleet start did not receive Fleet-planned command: %#v", request)
			}
			return fleetWire(map[string]any{"effects": []any{leaseIntent}})
		case "fleet/fleet.v1.admin.direct-provision.effect.claim":
			request := decodeAdminPaymentPayload(t, payload)
			if request["effect_id"] != "provision-effect-1" || request["consumer_id"] != "direct-request-1:direct-provision" || request["lease_duration_millis"] != int64(30000) {
				t.Fatalf("targeted Fleet claim = %#v", request)
			}
			return fleetWire(map[string]any{"lease": map[string]any{"lease_id": "lease-1", "intent": leaseIntent}})
		case "effects/effects.execute.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["id"] != "provision-effect-1" || request["kind"] != "fleet.server.provision" || request["idempotency_key"] != "provision-key-1" {
				t.Fatalf("leased provision executor request = %#v", request)
			}
			return fleetWire(map[string]any{"intent_id": "provision-effect-1", "kind": "fleet.server.provision", "idempotency_key": "provision-key-1", "status": "completed", "result": []byte{0x80}})
		case "fleet/fleet.v1.admin.direct-provision.receipt.apply":
			request := decodeAdminPaymentPayload(t, payload)
			receipt := request["receipt"].(map[string]any)
			if request["consumer_id"] != "direct-request-1:direct-provision" || request["lease_id"] != "lease-1" || receipt["intent_id"] != "provision-effect-1" {
				t.Fatalf("Fleet receipt apply is not fenced to lease/intent: %#v", request)
			}
			return fleetWire(map[string]any{"server": map[string]any{"id": "server-1", "order_id": "order-1"}, "commerce": commerceFact, "effect_id": "provision-effect-1", "kind": "fleet.server.provision", "receipt": receipt})
		case "commerce/commerce.admin.payment-provision.http.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			fleetTerminal := request["fleet"].(map[string]any)
			if request["operation"] != "direct_provision" || fleetTerminal["server_id"] != "server-1" || fleetTerminal["order_id"] != "order-1" || fleetTerminal["effect_id"] != "provision-effect-1" || fleetTerminal["commerce"] != nil || fleetTerminal["server"] != nil {
				t.Fatalf("Commerce projector did not receive normalized Fleet terminal evidence: %#v", request)
			}
			return adminPaymentProjection()
		default:
			return nil, fmt.Errorf("unexpected direct-provision call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.admin.provision.direct.final-http.v1", Payload: map[string]any{
		"request_id": "direct-request-1", "actor": map[string]any{"id": "admin-1", "is_admin": true},
		"command": map[string]any{"idempotency_key": "direct-key-1", "provision_id": "provision-1", "template": "paper", "email": "player@example.test", "username": "player"},
	}}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.admin.provision.direct-order.prepare.v1", "control/control.v1.query",
		"fleet/fleet.v1.admin.direct-provision.plan", "fleet/fleet.v1.admin.direct-provision.start",
		"fleet/fleet.v1.admin.direct-provision.effect.claim", "effects/effects.execute.v1",
		"fleet/fleet.v1.admin.direct-provision.receipt.apply", "commerce/commerce.admin.payment-provision.http.project.v1",
	)
}
