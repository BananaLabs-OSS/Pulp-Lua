package orchestrator

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func normalCheckoutGatherCalls() []string {
	return []string{
		"sessions/sessions.gene.route-intent.parse.v1",
		"control/control.checkout.offer.resolve.v1",
		"control/control.v1.eula.get",
		"fleet/fleet.v1.query.preflight.upload-datapacks",
		"commerce/commerce.checkout.normal.quote.v1",
		"sessions/sessions.checkout.preflight.v1",
	}
}

func normalCheckoutRouteIntent(t *testing.T, payload []byte, ageConfirmed bool) []byte {
	t.Helper()
	var request map[string]any
	if err := msgpack.Unmarshal(payload, &request); err != nil {
		t.Fatalf("decode route intent request: %v", err)
	}
	if request["method"] != "POST" || request["path"] != "/api/checkout" {
		t.Fatalf("route intent request = %#v", request)
	}
	wire, err := msgpack.Marshal(map[string]any{
		"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
		"checkout": map[string]any{
			"kind": "normal",
			"normal": map[string]any{
				"server_type": "paper", "tier_id": "tier-1", "engine": "paper",
				"version": "1.21.5", "email": "player@example.test",
				"normalized_email": "player@example.test", "username": "Player",
				"promo_code": "SAVE", "normalized_promotion_code": "SAVE",
				"gamemode": "survival", "difficulty": "normal", "pvp": "true",
				"hardcore": "false", "motd": "Hello", "upload_id": "upload-1",
				"datapack_ids": []any{"datapack-1", "datapack-2"},
				"mods_json":    "[]", "duration_days": int64(30),
				"age_confirmed": ageConfirmed, "eula_accepted": true,
				"tos_accepted": true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func normalCheckoutOffer(image string) map[string]any {
	return map[string]any{
		"version": "sessions.control/v1", "approved": true, "http_status": int64(200),
		"game_id": "minecraft", "template": "paper", "tier_id": "tier-1",
		"duration_days": int64(30), "price_cents": int64(1200),
		"currency": "usd", "revision": int64(9),
		"config": map[string]any{
			"valid": true, "control_revision": int64(9),
			"runtime_resolution": map[string]any{
				"version": "sessions.control/v1", "game_id": "minecraft",
				"template": "paper", "tier_id": "tier-1",
				"max_cpu": float64(1.5), "max_ram_mb": int64(1536),
				"runtime_revision": int64(7), "control_revision": int64(9),
				"approved_image": map[string]any{
					"reference": image, "approval_id": "approval-1",
					"policy_version": "sessions-runtime-v1", "approved": true,
				},
			},
		},
	}
}

func TestNormalCheckoutCompositionFailsClosedAtSessionsPreflight(t *testing.T) {
	const image = "registry.example/paper@sha256:" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	var calls []string
	runtime, err := New(Options{
		Script: evolutionLua(t),
		Caller: CallFunc(func(target, function string, payload []byte) ([]byte, error) {
			calls = append(calls, target+"/"+function)
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			switch target + "/" + function {
			case "sessions/sessions.gene.route-intent.parse.v1":
				return normalCheckoutRouteIntent(t, payload, false), nil
			case "control/control.checkout.offer.resolve.v1":
				return msgpack.Marshal(normalCheckoutOffer(image))
			case "control/control.v1.eula.get":
				if request["game_id"] != "minecraft" {
					t.Fatalf("Control EULA request = %#v", request)
				}
				return msgpack.Marshal(map[string]any{
					"version": "sessions.control/v1", "required": true, "revision": int64(9),
				})
			case "fleet/fleet.v1.query.preflight.upload-datapacks":
				return msgpack.Marshal(map[string]any{
					"allowed": true, "upload_id": "upload-1",
					"datapack_ids": []any{"datapack-1", "datapack-2"},
				})
			case "commerce/commerce.checkout.normal.quote.v1":
				return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
					"fingerprint": "quote-fingerprint-1", "original_amount_cents": int64(1200),
					"discount_cents": int64(0), "amount_cents": int64(1200),
					"currency": "usd", "quoted_at_unix": int64(1_750_000_000),
				}})
			case "sessions/sessions.checkout.preflight.v1":
				facts := request["checkout_facts"].(map[string]any)
				if facts["verified"] != true || facts["uploads_validated"] != true ||
					facts["requires_eula"] != true {
					t.Fatalf("Sessions checkout facts = %#v", facts)
				}
				return msgpack.Marshal(map[string]any{
					"version": "sessions.checkout.preflight.v1", "http_status": int64(400),
					"http_error": "Age confirmation required.",
				})
			default:
				t.Fatalf("unexpected call %s/%s", target, function)
				return nil, nil
			}
		}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	requestWire, err := msgpack.Marshal(map[string]any{
		"method": "POST", "path": "/api/checkout", "body": []byte(`{"age_confirmed":false}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := workflow.NewSagaRequest(
		"evolution.sessions.checkout.v1", "normal-preflight-1", "normal-preflight-key-1",
		map[string]any{"request_msgpack": string(requestWire)},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("ExecuteSaga: %v", err)
	}
	response := decodeCheckoutResponse(t, result)
	if response.Status != 400 || !strings.Contains(string(response.Body), "Age confirmation required.") {
		t.Fatalf("response = %#v body=%s", response, response.Body)
	}
	if strings.Join(calls, "\n") != strings.Join(normalCheckoutGatherCalls(), "\n") {
		t.Fatalf("preflight calls = %v, want %v", calls, normalCheckoutGatherCalls())
	}
}

func TestNormalCheckoutCompositionUsesExactOwnerContracts(t *testing.T) {
	testNormalCheckoutCompositionExactOwnerContracts(t, false, false, false)
}

func TestNormalCheckoutCompositionAppliesFailureBeforeFleetCompensation(t *testing.T) {
	testNormalCheckoutCompositionExactOwnerContracts(t, true, false, false)
}

func TestNormalCheckoutCompositionExposesPostActionsAsPendingHostWork(t *testing.T) {
	testNormalCheckoutCompositionExactOwnerContracts(t, false, true, false)
}

func TestNormalCheckoutCompositionFreeCompositeExposesAllPostActions(t *testing.T) {
	testNormalCheckoutCompositionExactOwnerContracts(t, false, false, true)
}

func testNormalCheckoutCompositionExactOwnerContracts(t *testing.T, failEffect, postAction, free bool) {
	const image = "registry.example/paper@sha256:" +
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	var calls []string
	quoteCalls := 0
	freeEffectCalls := 0
	caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		calls = append(calls, target+"/"+function)
		var command map[string]any
		if len(payload) > 0 {
			if err := msgpack.Unmarshal(payload, &command); err != nil {
				t.Fatal(err)
			}
		}
		switch target + "/" + function {
		case "sessions/sessions.gene.route-intent.parse.v1":
			return normalCheckoutRouteIntent(t, payload, true), nil
		case "sessions/sessions.checkout.preflight.v1":
			facts := command["checkout_facts"].(map[string]any)
			if facts["verified"] != true || facts["observed_at_unix"] != int64(1_750_000_000) ||
				facts["resolved_server_type"] != "paper" || facts["resolved_tier_id"] != "tier-1" ||
				facts["uploads_validated"] != true || facts["requires_eula"] != true {
				t.Fatalf("Sessions checkout facts = %#v", facts)
			}
			return msgpack.Marshal(map[string]any{
				"version": "sessions.checkout.preflight.v1", "http_status": int64(200),
				"request_id": "checkout-request-1", "idempotency_key": "checkout-key-1",
				"client_ip": "203.0.113.8",
				"claim": map[string]any{
					"id": "claim-1", "email": "player@example.test", "token": "claim-token-1",
					"created_at_unix": int64(1_750_000_000), "expires_at_unix": int64(1_750_007_200),
				},
				"owner": map[string]any{
					"order_id": "order-1", "email": "player@example.test", "server_type": "paper",
					"tier_id": "tier-1", "amount_cents": int64(777), "currency": "cad",
					"description": "Minecraft Server (paper)", "age_confirmed": true,
					"tos_accepted": true, "eula_accepted": true, "auto_redeem": true,
					"config": map[string]any{
						"username": "Player", "gamemode": "survival", "difficulty": "normal",
						"pvp": "true", "hardcore": "false", "motd": "Hello",
					},
				},
			})
		case "control/control.v1.checkout_rate_limit.consume":
			return msgpack.Marshal(map[string]any{
				"version": "sessions.control/v1", "allowed": true, "http_status": int64(200),
			})
		case "control/control.checkout.offer.resolve.v1":
			if command["version"] != "sessions.control/v1" || command["server_type"] != "paper" {
				t.Fatalf("Control offer request = %#v", command)
			}
			settings := command["settings"].(map[string]any)
			if settings["username"] != nil || settings["gamemode"] != "survival" {
				t.Fatalf("Control runtime settings = %#v", settings)
			}
			return msgpack.Marshal(normalCheckoutOffer(image))
		case "control/control.v1.eula.get":
			if command["version"] != "sessions.control/v1" || command["game_id"] != "minecraft" {
				t.Fatalf("Control EULA request = %#v", command)
			}
			return msgpack.Marshal(map[string]any{
				"version": "sessions.control/v1", "required": true, "revision": int64(9),
			})
		case "fleet/fleet.v1.query.preflight.upload-datapacks":
			if command["upload_id"] != "upload-1" {
				t.Fatalf("Fleet upload fact request = %#v", command)
			}
			return msgpack.Marshal(map[string]any{
				"allowed": true, "upload_id": "upload-1",
				"datapack_ids": []any{"datapack-1", "datapack-2"},
			})
		case "commerce/commerce.checkout.normal.quote.v1":
			quoteCalls++
			if command["original_amount_cents"] != int64(1200) || command["currency"] != "usd" {
				t.Fatalf("Commerce quote query = %#v", command)
			}
			if quoteCalls == 1 && command["now_unix"] != nil {
				t.Fatalf("Commerce fact quote invented a clock: %#v", command)
			}
			if quoteCalls == 2 && command["now_unix"] != int64(1_750_000_000) {
				t.Fatalf("Commerce admission quote has wrong owner time: %#v", command)
			}
			discount, amount := int64(0), int64(1200)
			if free {
				discount, amount = 1200, 0
			}
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
				"fingerprint": "quote-fingerprint-1", "original_amount_cents": int64(1200),
				"discount_cents": discount, "amount_cents": amount,
				"currency": "usd", "quoted_at_unix": int64(1_750_000_000),
			}})
		case "identity/identity.checkout.authorize-register.v1":
			claim := command["claim"].(map[string]any)
			if claim["created_at"] != int64(1_750_000_000_000) ||
				command["now"] != int64(1_750_000_000_000) {
				t.Fatalf("Identity time units = %#v", command)
			}
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
				"version": "identity.checkout.v1", "subject_id": "claim-1",
				"email": "player@example.test", "email_verified": true,
				"claim_token": "claim-token-1",
				"consent": map[string]any{
					"id": "checkout-request-1:consent", "checkout_id": "checkout-request-1",
					"claim_id": "claim-1", "age_confirmed": true, "tos_accepted": true,
				},
			}})
		case "fleet/fleet.v1.command.checkout.reserve":
			plan := command["launch_plan"].(map[string]any)
			if plan["version"] != "fleet.checkout.launch-plan.v1" ||
				plan["cpu_millis"] != int64(1500) || plan["memory_mb"] != int64(1536) ||
				plan["runtime_revision"] != int64(7) || plan["control_revision"] != int64(9) {
				t.Fatalf("Fleet launch plan = %#v", plan)
			}
			return msgpack.Marshal(checkoutFleetDecision(image))
		case "fleet/fleet.v1.command.checkout.recheck":
			if command["lease_id"] != "checkout:order-1-reservation:lease:3" ||
				command["expected_revision"] != int64(3) {
				t.Fatalf("Fleet recheck fence = %#v", command)
			}
			return msgpack.Marshal(checkoutFleetDecision(image))
		case "commerce/commerce.checkout.normal.admit.v1":
			quote := command["quote"].(map[string]any)
			compat := command["compatibility"].(map[string]any)
			expectedMax := int64(1200)
			if free {
				expectedMax = 0
			}
			if command["idempotency_key"] != "checkout-key-1" ||
				command["identity_authorization_id"] != "checkout-request-1:consent" ||
				command["fleet_authorization_id"] != "checkout:order-1-reservation:lease:3" ||
				command["fleet_reservation_id"] != "order-1-reservation" ||
				quote["fingerprint"] != "quote-fingerprint-1" ||
				compat["max_amount_cents"] != expectedMax ||
				compat["age_confirmed_at_unix"] != int64(1_750_000_000) {
				t.Fatalf("Commerce admit command = %#v", command)
			}
			var intent any
			if free {
				typedIntent, err := effect.NewIntent(
					"checkout:checkout-request-1:free-invoice",
					effect.KindStripeFreeInvoiceFinalize,
					"checkout-key-1:payment",
					effect.StripeFreeInvoiceFinalizePayload{
						Customer: effect.StripeCustomerCreatePayload{
							Email: "player@example.test", Description: "Minecraft Server (paper)",
							Metadata: map[string]string{"order_id": "order-1", "checkout_id": "checkout-request-1"},
						},
						InvoiceItem: effect.StripeFreeInvoiceItem{
							AmountCents: 1200, Currency: "usd", Description: "Minecraft Server (paper)",
						},
						Invoice: effect.StripeFreeInvoice{
							Description: "Minecraft Server (paper)", CollectionMethod: "charge_automatically",
							Metadata:        map[string]string{"order_id": "order-1", "checkout_id": "checkout-request-1"},
							PromotionCodeID: "promo_1",
						},
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				intent = typedIntent
			} else {
				intentPayload, _ := msgpack.Marshal(map[string]any{"amount_cents": int64(1200)})
				intent = map[string]any{
					"id":              "checkout:checkout-request-1:stripe",
					"kind":            "pulp.effect.stripe.payment-intent.create.v1",
					"idempotency_key": "checkout-key-1:payment", "payload": intentPayload,
				}
			}
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
				"checkout": map[string]any{"effects": []any{intent}},
				"effects":  []any{intent},
				"compensation_plan": []any{map[string]any{
					"kind": "fleet.checkout.reservation.release", "order_id": "order-1",
					"checkout_id":    "checkout-request-1",
					"decision_id":    "checkout:order-1-reservation:lease:3",
					"reservation_id": "order-1-reservation",
				}},
			}})
		case "effects/effects.execute.v1":
			intent, err := effect.UnmarshalIntent(payload)
			if err != nil {
				t.Fatal(err)
			}
			var result any
			if free {
				// The checkout composition owns the free-invoice dependency chain.
				// Each provider operation has a receipt-bound intent; Commerce only
				// sees the synthesized receipt for the original compound intent.
				freeEffectCalls++
				step := freeEffectCalls
				expected := []struct {
					id, kind, key string
					payload       map[string]any
					result        map[string]any
				}{
					{
						id:   "checkout:checkout-request-1:free-invoice:customer",
						kind: "pulp.effect.stripe.customer.create.v1", key: "checkout-key-1:payment:customer",
						payload: map[string]any{
							"email": "player@example.test", "description": "Minecraft Server (paper)",
							"metadata": map[string]any{"order_id": "order-1", "checkout_id": "checkout-request-1"},
						},
						result: map[string]any{"customer_id": "cus_1"},
					},
					{
						id:   "checkout:checkout-request-1:free-invoice:item",
						kind: "pulp.effect.stripe.invoice-item.create.v1", key: "checkout-key-1:payment:item",
						payload: map[string]any{
							"customer_id": "cus_1", "amount_cents": int64(1200), "currency": "usd",
							"description": "Minecraft Server (paper)",
						},
						result: map[string]any{"invoice_item_id": "ii_1"},
					},
					{
						id:   "checkout:checkout-request-1:free-invoice:invoice",
						kind: "pulp.effect.stripe.invoice.create.v1", key: "checkout-key-1:payment:invoice",
						payload: map[string]any{
							"customer_id": "cus_1", "description": "Minecraft Server (paper)",
							"auto_advance": false, "collection_method": "charge_automatically",
							"metadata":          map[string]any{"order_id": "order-1", "checkout_id": "checkout-request-1"},
							"promotion_code_id": "promo_1",
						},
						result: map[string]any{"invoice_id": "in_1", "status": "draft"},
					},
					{
						id:   "checkout:checkout-request-1:free-invoice:finalize",
						kind: "pulp.effect.stripe.invoice.finalize.v1", key: "checkout-key-1:payment:finalize",
						payload: map[string]any{"invoice_id": "in_1"},
						result:  map[string]any{"invoice_id": "in_1", "status": "open", "amount_due": int64(0)},
					},
					{
						id:   "checkout:checkout-request-1:free-invoice:paid",
						kind: "pulp.effect.stripe.invoice.mark-paid.v1", key: "checkout-key-1:payment:paid",
						payload: map[string]any{"invoice_id": "in_1"},
						result: map[string]any{
							"invoice_id": "in_1", "status": "paid", "amount_due": int64(0), "amount_paid": int64(0),
						},
					},
				}
				if step > len(expected) {
					t.Fatalf("unexpected free invoice unit effect %d: %#v", step, intent)
				}
				want := expected[step-1]
				if intent.ID != want.id || intent.Kind != want.kind || intent.IdempotencyKey != want.key {
					t.Fatalf("free invoice unit intent %d = %#v, want id=%q kind=%q key=%q", step, intent, want.id, want.kind, want.key)
				}
				var gotPayload map[string]any
				if err := msgpack.Unmarshal(intent.Payload, &gotPayload); err != nil {
					t.Fatalf("decode free invoice unit payload %d: %v", step, err)
				}
				if !reflect.DeepEqual(gotPayload, want.payload) {
					t.Fatalf("free invoice unit payload %d = %#v, want %#v", step, gotPayload, want.payload)
				}
				result = want.result
			} else if intent.ID != "checkout:checkout-request-1:stripe" ||
				intent.IdempotencyKey != "checkout-key-1:payment" {
				t.Fatalf("effect intent = %#v", intent)
			}
			var receipt effect.Receipt
			if failEffect {
				receipt, err = effect.NewFailedReceipt(intent, effect.Failure{
					Code: "stripe_unavailable", Message: "Stripe is unavailable",
				})
			} else if free {
				receipt, err = effect.NewCompletedReceipt(intent, result)
			} else {
				receipt, err = effect.NewCompletedReceipt(intent, map[string]any{
					"id": "pi_123", "client_secret": "pi_secret_123",
					"status": "requires_payment_method",
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			return effect.MarshalReceipt(receipt)
		case "commerce/commerce.checkout.effect.apply.v1":
			receipt := command["receipt"].(map[string]any)
			expectedID := "checkout:checkout-request-1:stripe"
			if free {
				expectedID = "checkout:checkout-request-1:free-invoice"
			}
			if command["idempotency_key"] != "checkout-key-1:effect:1" ||
				command["effect_id"] != expectedID ||
				receipt["intent_id"] != expectedID {
				t.Fatalf("Commerce apply command = %#v", command)
			}
			if failEffect {
				return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
					"terminal": true,
					"compensations": []any{map[string]any{
						"kind": "fleet.checkout.reservation.release", "order_id": "order-1",
						"checkout_id":    "checkout-request-1",
						"decision_id":    "checkout:order-1-reservation:lease:3",
						"reservation_id": "order-1-reservation", "reason": "Stripe is unavailable",
					}},
				}})
			}
			if postAction {
				return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
					"terminal": true,
					"post_actions": []any{
						map[string]any{
							"kind": "storage.checkout.uploads.release", "order_id": "order-1",
							"checkout_id": "checkout-request-1",
							"compatibility": map[string]any{
								"upload_id": "upload-1", "datapack_ids": "datapack-1,datapack-2",
							},
						},
						map[string]any{
							"kind": "sessions.checkout.resolve.prewarm", "order_id": "order-1",
							"checkout_id": "checkout-request-1",
							"compatibility": map[string]any{
								"server_type": "paper", "engine": "paper", "version": "1.21.5",
								"mods_json": "[]", "datapack_ids": "datapack-1,datapack-2",
							},
						},
					},
				}})
			}
			if free {
				compatibility := map[string]any{
					"email": "player@example.test", "server_type": "paper",
					"engine": "paper", "version": "1.21.5",
					"upload_id": "upload-1", "datapack_ids": "datapack-1,datapack-2",
					"auto_redeem": true, "age_confirmed_at_unix": int64(1_750_000_000),
					"tos_accepted_at_unix": int64(1_750_000_000),
					"consent_snapshot":     "sessions.checkout.v1",
				}
				return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
					"terminal": true,
					"legacy_response": map[string]any{
						"claim_token": "claim-token-1", "discount_cents": int64(1200),
						"free": true, "order_id": "order-1",
					},
					"post_actions": []any{
						map[string]any{
							"kind": "storage.checkout.uploads.release", "order_id": "order-1",
							"checkout_id": "checkout-request-1", "compatibility": compatibility,
						},
						map[string]any{
							"kind": "sessions.checkout.resolve.prewarm", "order_id": "order-1",
							"checkout_id": "checkout-request-1", "compatibility": compatibility,
						},
						map[string]any{
							"kind": "notification.checkout.order-confirmed", "order_id": "order-1",
							"checkout_id": "checkout-request-1", "email": "player@example.test",
							"original_amount_cents": int64(1200), "discount_cents": int64(1200),
							"compatibility": compatibility,
						},
						map[string]any{
							"kind": "sessions.checkout.free-order.deploy", "order_id": "order-1",
							"checkout_id": "checkout-request-1", "email": "player@example.test",
							"compatibility": compatibility,
						},
					},
				}})
			}
			return msgpack.Marshal(map[string]any{"ok": true, "value": map[string]any{
				"terminal": true,
				"legacy_response": map[string]any{
					"claim_token": "claim-token-1", "discount_cents": int64(0),
					"client_secret": "pi_secret_123",
				},
			}})
		case "fleet/fleet.v1.command.capacity.release":
			if !failEffect || command["reservation_id"] != "order-1-reservation" ||
				command["lease_id"] != "checkout:order-1-reservation:lease:3" ||
				command["expected_revision"] != int64(3) {
				t.Fatalf("Fleet compensation command = %#v", command)
			}
			return msgpack.Marshal(map[string]any{
				"reservation": map[string]any{"id": "order-1-reservation", "status": "released"},
			})
		default:
			t.Fatalf("unexpected call %s/%s", target, function)
			return nil, nil
		}
	})

	runtime, err := New(Options{Script: evolutionLua(t), Caller: caller})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()
	requestWire, _ := msgpack.Marshal(map[string]any{
		"method": "POST", "path": "/api/checkout", "body": []byte(`{}`),
	})
	request, _ := workflow.NewSagaRequest(
		"evolution.sessions.checkout.v1", "normal-success-1", "normal-success-key-1",
		map[string]any{"request_msgpack": string(requestWire)},
	)
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("ExecuteSaga: %v", err)
	}
	if failEffect {
		if result.Status != workflow.SagaFailed || result.Error == nil ||
			result.Error.Code != "stripe_unavailable" {
			t.Fatalf("failed checkout result = %#v", result)
		}
	} else if postAction || free {
		expectedKinds := []string{
			"storage.checkout.uploads.release",
			"sessions.checkout.resolve.prewarm",
		}
		if free {
			expectedKinds = []string{
				"storage.checkout.uploads.release",
				"sessions.checkout.resolve.prewarm",
				"notification.checkout.order-confirmed",
				"sessions.checkout.free-order.deploy",
			}
		}
		if result.Status != workflow.SagaPending || len(result.Effects) != len(expectedKinds) {
			t.Fatalf("pending checkout post actions = %#v", result)
		}
		for index, expectedKind := range expectedKinds {
			effect := result.Effects[index]
			suffix := strconv.Itoa(index + 1)
			if effect.Kind != expectedKind ||
				effect.ID != "checkout:checkout-request-1:post:"+suffix ||
				effect.IdempotencyKey != "checkout-key-1:post:"+suffix {
				t.Fatalf("pending post action effect %d = %#v", index, effect)
			}
			var envelope map[string]any
			if err := msgpack.Unmarshal(effect.Payload, &envelope); err != nil {
				t.Fatalf("decode pending post action envelope %d: %v", index, err)
			}
			action := envelope["action"].(map[string]any)
			runtime := envelope["runtime"].(map[string]any)
			fleet := envelope["fleet"].(map[string]any)
			reservation := fleet["reservation"].(map[string]any)
			if envelope["version"] != "sessions.checkout.post-action.host.v1" ||
				envelope["idempotency_key"] != effect.IdempotencyKey ||
				action["kind"] != expectedKind || action["checkout_id"] != "checkout-request-1" ||
				runtime["game_id"] != "minecraft" || runtime["template"] != "paper" ||
				runtime["tier_id"] != "tier-1" || runtime["max_cpu"] != 1.5 ||
				runtime["max_ram_mb"] != int64(1536) ||
				fleet["lease_id"] != "checkout:order-1-reservation:lease:3" ||
				fleet["revision"] != int64(3) ||
				reservation["id"] != "order-1-reservation" {
				t.Fatalf("pending post action envelope %d = %#v", index, envelope)
			}
		}
	} else {
		response := decodeCheckoutResponse(t, result)
		if response.Status != 200 {
			t.Fatalf("response = %#v", response)
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body, &body); err != nil ||
			body["client_secret"] != "pi_secret_123" {
			t.Fatalf("body = %#v, %v", body, err)
		}
	}
	wantCalls := append(normalCheckoutGatherCalls(),
		"control/control.v1.checkout_rate_limit.consume",
		"control/control.checkout.offer.resolve.v1",
		"commerce/commerce.checkout.normal.quote.v1",
		"identity/identity.checkout.authorize-register.v1",
		"fleet/fleet.v1.command.checkout.reserve",
		"fleet/fleet.v1.command.checkout.recheck",
		"commerce/commerce.checkout.normal.admit.v1",
		"commerce/commerce.checkout.effect.apply.v1",
	)
	if free {
		wantCalls = append(wantCalls[:len(wantCalls)-1],
			"effects/effects.execute.v1",
			"effects/effects.execute.v1",
			"effects/effects.execute.v1",
			"effects/effects.execute.v1",
			"effects/effects.execute.v1",
			"commerce/commerce.checkout.effect.apply.v1",
		)
	} else {
		wantCalls = append(wantCalls[:len(wantCalls)-1],
			"effects/effects.execute.v1",
			"commerce/commerce.checkout.effect.apply.v1",
		)
	}
	if failEffect {
		wantCalls = append(wantCalls, "fleet/fleet.v1.command.capacity.release")
	}
	if strings.Join(calls, "\n") != strings.Join(wantCalls, "\n") {
		t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
	}
}

func checkoutFleetDecision(image string) map[string]any {
	return map[string]any{
		"reserved": true, "queued": false, "requires_recheck": true,
		"revision": int64(3), "lease_id": "checkout:order-1-reservation:lease:3",
		"reservation": map[string]any{
			"id": "order-1-reservation", "order_id": "order-1",
			"server_id": "order-1-server",
			"node_id":   "node-1",
			"revision":  int64(3), "lease_id": "checkout:order-1-reservation:lease:3",
		},
		"launch_plan": map[string]any{
			"version": "fleet.checkout.launch-plan.v1",
			"image": map[string]any{
				"reference": image, "approval_id": "approval-1",
				"policy_version": "sessions-runtime-v1", "approved": true,
			},
		},
	}
}
