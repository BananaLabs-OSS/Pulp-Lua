package orchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

type checkoutHTTPResponse struct {
	Status  uint32            `msgpack:"status"`
	Headers map[string]string `msgpack:"headers"`
	Body    []byte            `msgpack:"body"`
}

func evolutionLua(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "Sessions-Gene", "application", "sessions.lua")
	script, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(script)
}

func baseExtensionPlan(gift, free, instant bool) map[string]any {
	kind := "session-extension"
	mode := "queued"
	if gift {
		kind = "gift-extension"
	}
	if instant {
		mode = "instant"
	}
	order := map[string]any{
		"id": "order-extension-1", "original_order_id": "order-original-1",
		"server_type": "minecraft", "amount_cents": int64(1200), "currency": "usd",
		"extend_mode": mode, "extend_server_id": "server-1", "is_gift": gift,
	}
	if free {
		order["amount_cents"] = int64(0)
	}
	if gift {
		order["buyer_email"] = "buyer@example.test"
		order["gift_token"] = "gift-token-1"
	} else {
		order["email"] = "owner@example.test"
	}
	return map[string]any{
		"version": "sessions.checkout.preflight.v1", "http_status": int64(200),
		"request_id": "request-commerce-1", "idempotency_key": "checkout:extension-1",
		"free": free, "estimated_deploy": "2026-08-01T12:00:00Z",
		"order":  order,
		"server": map[string]any{"id": "server-1", "owner_order_id": "order-original-1", "expected_state": "expiring", "activate_instantly": instant},
		"coupon": map[string]any{},
		"claim":  map[string]any{"id": "claim-1", "email": "owner@example.test", "token": "claim-token-1"},
		"kind":   kind,
	}
}

func sagaFromPlan(plan map[string]any) map[string]any {
	effectIntent := map[string]any{
		"id":              "extension:request-commerce-1:stripe",
		"kind":            effect.KindStripePaymentIntentCreate,
		"idempotency_key": "claim-1:pi",
		"acknowledgement": map[string]any{"status": "pending"},
	}
	return map[string]any{
		"request_id": plan["request_id"], "idempotency_key": plan["idempotency_key"],
		"kind": plan["kind"], "status": "pending", "order": plan["order"],
		"server": plan["server"], "coupon": plan["coupon"], "claim": plan["claim"],
		"effects": []any{effectIntent},
	}
}

func extensionCompositionFactCalls(gift bool) []string {
	calls := []string{"sessions/sessions.gene.route-intent.parse.v1"}
	if gift {
		calls = append(calls, "fleet/fleet.v1.query.preflight.share-token")
	} else {
		calls = append(calls, "fleet/fleet.v1.query.preflight.extension-server")
	}
	calls = append(calls,
		"fleet/fleet.v1.query.preflight.extension-server",
		"commerce/commerce.gene.order-projection.get.v1",
	)
	if !gift {
		calls = append(calls, "funding/funding.extension.email.authorization.get.v1")
	}
	return append(calls,
		"control/control.extension-duration.resolve.v1",
		"control/control.catalog-price.resolve.v1",
		"commerce/commerce.sessions.extension.preflight.quote.v1",
		"fleet/fleet.v1.query.preflight.queue-projection",
		map[bool]string{
			false: "sessions/sessions.checkout.extension.preflight.v1",
			true:  "sessions/sessions.checkout.gift-extension.preflight.v1",
		}[gift],
	)
}

func extensionCompositionFacts(
	t *testing.T,
	gift bool,
	plan map[string]any,
	target, function string,
	payload []byte,
) ([]byte, bool) {
	t.Helper()
	key := target + "/" + function
	var request map[string]any
	if len(payload) > 0 {
		if err := msgpack.Unmarshal(payload, &request); err != nil {
			t.Fatalf("decode %s request: %v", key, err)
		}
	}
	marshal := func(value any) ([]byte, bool) {
		wire, err := msgpack.Marshal(value)
		if err != nil {
			t.Fatalf("encode %s response: %v", key, err)
		}
		return wire, true
	}
	switch key {
	case "sessions/sessions.gene.route-intent.parse.v1":
		wantPath := "/api/extend-checkout"
		intentKey := "extension"
		intent := map[string]any{
			"server_id": "server-1", "email": "owner@example.test",
			"normalized_email": "owner@example.test", "promo_code": "SAVE",
		}
		if gift {
			wantPath = "/api/gift-extend-checkout"
			intentKey = "gift_extension"
			intent = map[string]any{
				"share_token": "share-token-1", "email": "buyer@example.test",
				"normalized_email": "buyer@example.test", "promo_code": "SAVE",
			}
		}
		if request["method"] != "POST" || request["path"] != wantPath {
			t.Fatalf("route intent request = %#v", request)
		}
		return marshal(map[string]any{
			"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
			"checkout": map[string]any{"kind": map[bool]string{false: "extension", true: "gift-extension"}[gift], intentKey: intent},
		})
	case "fleet/fleet.v1.query.preflight.share-token":
		if !gift || request["share_token"] != "share-token-1" {
			t.Fatalf("Fleet share-token binding request = %#v", request)
		}
		return marshal(map[string]any{
			"found": true, "server_id": "server-1", "order_id": "order-original-1",
		})
	case "fleet/fleet.v1.query.preflight.extension-server":
		if request["server_id"] != "server-1" {
			t.Fatalf("Fleet extension-server request = %#v", request)
		}
		return marshal(map[string]any{
			"found": true, "server_id": "server-1", "order_id": "order-original-1",
			"state": "expiring", "expires_at_unix": int64(1_800_000_000),
			"container_id": "container-1", "ip": "10.0.0.2", "port": int64(25565),
		})
	case "commerce/commerce.gene.order-projection.get.v1":
		if request["order_id"] != "order-original-1" {
			t.Fatalf("Commerce extension projection request = %#v", request)
		}
		return marshal(map[string]any{"ok": true, "value": map[string]any{
			"server_type": "minecraft",
			"order": map[string]any{
				"id": "order-original-1", "tier_id": "tier-1",
				"config": map[string]any{"gamemode": "survival", "difficulty": "normal"},
			},
		}})
	case "funding/funding.extension.email.authorization.get.v1":
		if gift || request["server_id"] != "server-1" ||
			request["order_id"] != "order-original-1" ||
			request["email"] != "owner@example.test" {
			t.Fatalf("Funding extension authorization request = %#v", request)
		}
		return marshal(map[string]any{"authorized": true, "authorization_id": "funding-extension-1"})
	case "control/control.extension-duration.resolve.v1":
		if request["server_type"] != "minecraft" || request["tier_id"] != "tier-1" {
			t.Fatalf("Control extension duration request = %#v", request)
		}
		return marshal(map[string]any{
			"version": "sessions.control/v1", "duration_seconds": int64(2_592_000),
			"revision": int64(7),
		})
	case "control/control.catalog-price.resolve.v1":
		if request["server_id"] != "server-1" || request["order_id"] != "order-original-1" {
			t.Fatalf("Control extension price request = %#v", request)
		}
		return marshal(map[string]any{
			"version": "sessions.control/v1", "amount_cents": int64(1200),
			"currency": "usd", "revision": int64(8),
		})
	case "commerce/commerce.sessions.extension.preflight.quote.v1":
		if request["server_id"] != "server-1" || request["order_id"] != "order-original-1" {
			t.Fatalf("Commerce extension quote request = %#v", request)
		}
		return marshal(map[string]any{"ok": true, "value": map[string]any{
			"quoted_at_unix": int64(1_750_000_000), "amount_cents": int64(1200),
			"original_amount_cents": int64(1200), "discount_cents": int64(0),
			"currency": "usd",
		}})
	case "fleet/fleet.v1.query.preflight.queue-projection":
		if request["server_id"] != "server-1" || request["gift"] != gift {
			t.Fatalf("Fleet extension queue request = %#v", request)
		}
		mode := plan["order"].(map[string]any)["extend_mode"]
		return marshal(map[string]any{
			"found": true, "server_id": "server-1", "mode": mode,
			"estimated_deploy": "2026-08-01T12:00:00Z",
		})
	case "sessions/sessions.checkout.extension.preflight.v1",
		"sessions/sessions.checkout.gift-extension.preflight.v1":
		facts, ok := request["extension_facts"].(map[string]any)
		if !ok || facts["verified"] != true || facts["observed_at_unix"] != int64(1_750_000_000) {
			t.Fatalf("Sessions extension preflight facts = %#v", request)
		}
		if facts["email_owns_server"] != !gift || facts["share_token_matched"] != gift {
			t.Fatalf("Sessions extension authorization facts = %#v", facts)
		}
		return marshal(plan)
	default:
		return nil, false
	}
}

func decodeCheckoutResponse(t *testing.T, result workflow.SagaResult) checkoutHTTPResponse {
	t.Helper()
	wire, err := workflow.DecodeResult[[]byte](result)
	if err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	var response checkoutHTTPResponse
	if err := msgpack.Unmarshal(wire, &response); err != nil {
		t.Fatalf("decode HTTP response: %v", err)
	}
	return response
}

func TestEvolutionExtensionAndGiftCheckoutSagasAreIdempotent(t *testing.T) {
	for _, test := range []struct {
		name, event, preflight, commerce, apply string
		gift                                    bool
	}{
		{name: "session", event: "evolution.sessions.extend.checkout.v1", preflight: "sessions.checkout.extension.preflight.v1", commerce: "commerce.gene.extension.payment.prepare.v1", apply: "commerce.gene.extension.payment.apply.v1"},
		{name: "gift", event: "evolution.sessions.gift-extend.checkout.v1", preflight: "sessions.checkout.gift-extension.preflight.v1", commerce: "commerce.gene.gift-extension.payment.prepare.v1", apply: "commerce.gene.gift-extension.payment.apply.v1", gift: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := baseExtensionPlan(test.gift, false, false)
			plan["kind"] = map[bool]string{false: "session-extension", true: "gift-extension"}[test.gift]
			saga := sagaFromPlan(plan)
			calls := []string{}
			effectCalls := 0
			caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
				calls = append(calls, target+"/"+function)
				if response, handled := extensionCompositionFacts(t, test.gift, plan, target, function, payload); handled {
					return response, nil
				}
				switch target + "/" + function {
				case "commerce/" + test.commerce:
					return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{"saga": saga}})
				case "effects/effects.execute.v1":
					effectCalls++
					intent, err := effect.UnmarshalIntent(payload)
					if err != nil {
						t.Fatalf("canonical effect intent: %v", err)
					}
					if intent.ID != "extension:request-commerce-1:stripe" || intent.Kind != effect.KindStripePaymentIntentCreate || intent.IdempotencyKey != "claim-1:pi" {
						t.Fatalf("intent = %#v", intent)
					}
					var effectPayload map[string]any
					if err := msgpack.Unmarshal(intent.Payload, &effectPayload); err != nil || effectPayload["amount_cents"] != int64(1200) {
						t.Fatalf("effect payload = %#v, %v", effectPayload, err)
					}
					receipt, err := effect.NewCompletedReceipt(intent, map[string]any{"id": "pi_123", "client_secret": "pi_secret_123", "status": "requires_payment_method"})
					if err != nil {
						t.Fatal(err)
					}
					return effect.MarshalReceipt(receipt)
				case "commerce/" + test.apply:
					var apply map[string]any
					if err := msgpack.Unmarshal(payload, &apply); err != nil {
						t.Fatal(err)
					}
					if apply["idempotency_key"] != "claim-1:pi:apply" || apply["effect_id"] != "extension:request-commerce-1:stripe" {
						t.Fatalf("apply = %#v", apply)
					}
					legacy := map[string]any{"claim_token": "claim-token-1", "client_secret": "pi_secret_123"}
					if test.gift {
						legacy["amount_cents"] = int64(1200)
					} else {
						legacy["mode"] = "queued"
						legacy["estimated_deploy"] = "2026-08-01T12:00:00Z"
					}
					return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{"saga": map[string]any{"legacy_response": legacy}}})
				default:
					t.Fatalf("unexpected call %s/%s", target, function)
					return nil, nil
				}
			})
			runtime, err := New(Options{Caller: caller, Script: evolutionLua(t)})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer runtime.Close()
			path := "/api/extend-checkout"
			requestBody := []byte(`{"server_id":"server-1","email":"owner@example.test","promo_code":"SAVE"}`)
			if test.gift {
				path = "/api/gift-extend-checkout"
				requestBody = []byte(`{"share_token":"share-token-1","email":"buyer@example.test","promo_code":"SAVE"}`)
			}
			requestWire, _ := msgpack.Marshal(map[string]any{"method": "POST", "path": path, "body": requestBody})
			request, err := workflow.NewSagaRequest(test.event, "request-1", "route:checkout-1", map[string]any{"request_msgpack": requestWire})
			if err != nil {
				t.Fatal(err)
			}
			first, err := runtime.ExecuteSaga(request)
			if err != nil {
				t.Fatalf("ExecuteSaga: %v", err)
			}
			response := decodeCheckoutResponse(t, first)
			if response.Status != 200 {
				t.Fatalf("response = %#v", response)
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body, &body); err != nil || body["client_secret"] != "pi_secret_123" {
				t.Fatalf("body = %#v, %v", body, err)
			}
			if _, err := runtime.ExecuteSaga(request); err != nil {
				t.Fatalf("replay: %v", err)
			}
			wantCalls := append(extensionCompositionFactCalls(test.gift),
				"commerce/"+test.commerce,
				"effects/effects.execute.v1",
				"commerce/"+test.apply,
			)
			if effectCalls != 1 || strings.Join(calls, "\n") != strings.Join(wantCalls, "\n") {
				t.Fatalf("replay performed effects: effectCalls=%d calls=%v want=%v", effectCalls, calls, wantCalls)
			}
		})
	}
}

func TestEvolutionInstantFreeExtensionCompletesWithoutStripe(t *testing.T) {
	plan := baseExtensionPlan(false, true, true)
	plan["kind"] = "session-extension"
	saga := sagaFromPlan(plan)
	saga["status"] = "completed"
	saga["legacy_response"] = map[string]any{"claim_token": "claim-token-1", "mode": "instant", "free": true}
	saga["effects"] = []any{
		map[string]any{"id": "extension:request-commerce-1:fleet", "kind": "pulp.effect.fleet.extension.apply.v1", "idempotency_key": "extension:order-extension-1", "acknowledgement": map[string]any{"status": "pending"}},
		map[string]any{"id": "extension:request-commerce-1:notification", "kind": "pulp.effect.notification.email.send.v1", "idempotency_key": "extension-notification:order-extension-1", "acknowledgement": map[string]any{"status": "pending"}},
	}
	calls := []string{}
	caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		calls = append(calls, target+"/"+function)
		if response, handled := extensionCompositionFacts(t, false, plan, target, function, payload); handled {
			return response, nil
		}
		if target == "commerce" && function == "commerce.gene.extension.payment.prepare.v1" {
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{"saga": saga}})
		}
		t.Fatalf("instant extension called %s/%s", target, function)
		return nil, nil
	})
	runtime, err := New(Options{Caller: caller, Script: evolutionLua(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	requestWire, _ := msgpack.Marshal(map[string]any{
		"method": "POST", "path": "/api/extend-checkout",
		"body": []byte(`{"server_id":"server-1","email":"owner@example.test","promo_code":"SAVE"}`),
	})
	request, _ := workflow.NewSagaRequest("evolution.sessions.extend.checkout.v1", "request-1", "route:free", map[string]any{"request_msgpack": requestWire})
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatal(err)
	}
	response := decodeCheckoutResponse(t, result)
	wantCalls := append(extensionCompositionFactCalls(false), "commerce/commerce.gene.extension.payment.prepare.v1")
	if response.Status != 200 || !strings.Contains(string(response.Body), `"free":true`) ||
		strings.Join(calls, "\n") != strings.Join(wantCalls, "\n") {
		t.Fatalf("response=%#v calls=%v want=%v", response, calls, wantCalls)
	}
}

func TestEvolutionExtensionFailureIsAppliedOnce(t *testing.T) {
	plan := baseExtensionPlan(false, false, false)
	plan["kind"] = "session-extension"
	saga := sagaFromPlan(plan)
	effectCalls, applyCalls := 0, 0
	var calls []string
	caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		calls = append(calls, target+"/"+function)
		if response, handled := extensionCompositionFacts(t, false, plan, target, function, payload); handled {
			return response, nil
		}
		switch target + "/" + function {
		case "commerce/commerce.gene.extension.payment.prepare.v1":
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{"saga": saga}})
		case "commerce/commerce.gene.extension.payment.apply.v1":
			applyCalls++
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{"saga": map[string]any{"status": "failed"}}})
		case "effects/effects.execute.v1":
			effectCalls++
			intent, err := effect.UnmarshalIntent(payload)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := effect.NewFailedReceipt(intent, effect.Failure{Code: "host_unavailable", Message: "provider unavailable"})
			if err != nil {
				t.Fatal(err)
			}
			return effect.MarshalReceipt(receipt)
		}
		t.Fatalf("unexpected call %s/%s", target, function)
		return nil, nil
	})
	runtime, err := New(Options{Caller: caller, Script: evolutionLua(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	requestWire, _ := msgpack.Marshal(map[string]any{
		"method": "POST", "path": "/api/extend-checkout",
		"body": []byte(`{"server_id":"server-1","email":"owner@example.test","promo_code":"SAVE"}`),
	})
	request, _ := workflow.NewSagaRequest("evolution.sessions.extend.checkout.v1", "request-1", "route:failure", map[string]any{"request_msgpack": requestWire})
	result, err := runtime.ExecuteSaga(request)
	if err != nil || result.Status != workflow.SagaFailed || result.Error == nil || result.Error.Message != "failed to create payment" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if _, err := runtime.ExecuteSaga(request); err != nil {
		t.Fatal(err)
	}
	if effectCalls != 1 || applyCalls != 1 {
		t.Fatalf("failure replay duplicated work: effects=%d apply=%d", effectCalls, applyCalls)
	}
	wantCalls := append(extensionCompositionFactCalls(false),
		"commerce/commerce.gene.extension.payment.prepare.v1",
		"effects/effects.execute.v1",
		"commerce/commerce.gene.extension.payment.apply.v1",
	)
	if strings.Join(calls, "\n") != strings.Join(wantCalls, "\n") {
		t.Fatalf("failure call sequence = %v, want %v", calls, wantCalls)
	}
}
