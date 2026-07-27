package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

// These tests deliberately exercise the public Evolution event names.  The
// route adapter supplies transport only; the Lua application must obtain every
// authorization and product/runtime fact from its owning package before it can
// invoke a Fleet command.

func TestFleetPublicAvailabilityComposesCommerceControlAndFleet(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "sessions/sessions.gene.route-intent.parse.v1":
			assertFleetRouteRequest(t, payload, "GET", "/api/voucher/order-1/availability")
			return fleetWire(map[string]any{
				"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
				"route": "availability", "order_id": "order-1", "template": "paper",
				"request_id": "request-1", "now_unix": int64(1_750_000_000), "now_rfc3339": "2025-06-15T15:06:40Z",
			})
		case "commerce/commerce.gene.order-projection.get.v1":
			assertFleetField(t, payload, "order_id", "order-1")
			return fleetWire(fleetCommerceResult())
		case "sessions/sessions.gene.commerce-plan.map.v1":
			return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
		case "control/control.v1.query":
			return fleetWire(fleetControlProjection())
		case "control/control.v1.config.validate":
			return fleetWire(fleetValidConfig())
		case "fleet/fleet.v1.query.gene.availability":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["plan"] == nil {
				t.Fatalf("availability request has no Commerce plan: %#v", request)
			}
			return fleetWire(fleetHTTPProjection())
		case "sessions/sessions.gene.fleet-response.project.v1":
			return fleetWire(fleetHTTPProjection())
		default:
			return nil, fmt.Errorf("unexpected public availability call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	executeFleetPublicEvent(t, runtime, "evolution.sessions.voucher.availability.get.v1", fleetHTTPRequest("GET", "/api/voucher/order-1/availability", nil, map[string]any{"template": "paper"}))
	assertFleetCalls(t, calls,
		"sessions/sessions.gene.route-intent.parse.v1",
		"commerce/commerce.gene.order-projection.get.v1",
		"sessions/sessions.gene.commerce-plan.map.v1",
		"control/control.v1.query",
		"control/control.v1.config.validate",
		"fleet/fleet.v1.query.gene.availability",
		"sessions/sessions.gene.fleet-response.project.v1",
	)
}

func TestFleetPublicSessionsListingComposesIdentityCommerceAndFleet(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "sessions/sessions.gene.route-intent.parse.v1":
			assertFleetRouteRequest(t, payload, "GET", "/api/sessions")
			return fleetWire(map[string]any{
				"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
				"route": "sessions", "claim_token": "route-token-1",
				"request_id": "request-1", "now_unix": int64(1_750_000_000), "now_rfc3339": "2025-06-15T15:06:40Z",
			})
		case "identity/identity.route-principal.resolve.v1":
			assertFleetField(t, payload, "token", "route-token-1")
			return fleetWire(fleetPrincipalResult())
		case "commerce/commerce.public.order-projection.list.v1":
			assertFleetField(t, payload, "subject_id", "subject-1")
			return fleetWire(map[string]any{"ok": true, "value": []any{fleetCommercePlan()}})
		case "sessions/sessions.gene.commerce-plan.map.v1":
			return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
		case "fleet/fleet.v1.query.gene.sessions":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["plans"] == nil || request["proven"] != true {
				t.Fatalf("sessions request is not identity-proven: %#v", request)
			}
			return fleetWire(fleetHTTPProjection())
		case "sessions/sessions.gene.fleet-response.project.v1":
			return fleetWire(fleetHTTPProjection())
		default:
			return nil, fmt.Errorf("unexpected public sessions call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	executeFleetPublicEvent(t, runtime, "evolution.sessions.sessions.get.v1", fleetHTTPRequest("GET", "/api/sessions", nil, map[string]any{"token": "route-token-1"}))
	assertFleetCalls(t, calls,
		"sessions/sessions.gene.route-intent.parse.v1",
		"identity/identity.route-principal.resolve.v1",
		"commerce/commerce.public.order-projection.list.v1",
		"sessions/sessions.gene.commerce-plan.map.v1",
		"fleet/fleet.v1.query.gene.sessions",
		"sessions/sessions.gene.fleet-response.project.v1",
	)
}

func TestFleetPublicConfigComposesAuthorizationAndValidatedRuntime(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "sessions/sessions.gene.route-intent.parse.v1":
			assertFleetRouteRequest(t, payload, "POST", "/api/voucher/order-1/config")
			return fleetWire(map[string]any{
				"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
				"route": "config", "order_id": "order-1", "claim_token": "route-token-1",
				"server_id": "server-1", "config": map[string]any{"motd": "hello"},
				"request_id": "request-1", "now_unix": int64(1_750_000_000), "now_rfc3339": "2025-06-15T15:06:40Z",
			})
		case "identity/identity.route-principal.resolve.v1":
			return fleetWire(fleetPrincipalResult())
		case "commerce/commerce.public.order-projection.get.v1":
			assertFleetField(t, payload, "order_id", "order-1")
			return fleetWire(fleetCommerceResult())
		case "commerce/commerce.public.order-projection.list.v1":
			return fleetWire(map[string]any{"ok": true, "value": []any{fleetCommercePlan()}})
		case "sessions/sessions.gene.commerce-plan.map.v1":
			return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
		case "fleet/fleet.v1.query.authorization.context":
			assertFleetField(t, payload, "operation", "gene.config")
			return fleetWire(fleetAuthorizationContext())
		case "control/control.v1.query":
			return fleetWire(fleetControlProjection())
		case "control/control.v1.config.validate":
			assertFleetField(t, payload, "operation", "deploy")
			return fleetWire(fleetValidConfig())
		case "fleet/fleet.v1.command.gene.config":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["plan"] == nil || request["config"] == nil {
				t.Fatalf("config command lacks composed facts: %#v", request)
			}
			return fleetWire(fleetHTTPProjection())
		case "sessions/sessions.gene.fleet-response.project.v1":
			return fleetWire(fleetHTTPProjection())
		default:
			return nil, fmt.Errorf("unexpected public config call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	executeFleetPublicEvent(t, runtime, "evolution.sessions.voucher.config.v1", fleetHTTPRequest("POST", "/api/voucher/order-1/config", map[string]any{"motd": "hello"}, map[string]any{"token": "route-token-1"}))
	assertFleetCalls(t, calls,
		"sessions/sessions.gene.route-intent.parse.v1",
		"identity/identity.route-principal.resolve.v1",
		"commerce/commerce.public.order-projection.list.v1",
		"commerce/commerce.public.order-projection.get.v1",
		"fleet/fleet.v1.query.authorization.context",
		"sessions/sessions.gene.commerce-plan.map.v1",
		"control/control.v1.query",
		"control/control.v1.config.validate",
		"fleet/fleet.v1.command.gene.config",
		"sessions/sessions.gene.fleet-response.project.v1",
	)
}

func TestFleetPublicPathsFailClosedBeforeFleetForIdentityOrConfigFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		identityOK bool
		configOK   bool
		wantCalls  []string
	}{
		{
			name: "identity", identityOK: false, configOK: true,
			wantCalls: []string{
				"sessions/sessions.gene.route-intent.parse.v1",
				"identity/identity.route-principal.resolve.v1",
			},
		},
		{
			name: "config", identityOK: true, configOK: false,
			wantCalls: []string{
				"sessions/sessions.gene.route-intent.parse.v1",
				"identity/identity.route-principal.resolve.v1",
				"commerce/commerce.public.order-projection.list.v1",
				"commerce/commerce.public.order-projection.get.v1",
				"fleet/fleet.v1.query.authorization.context",
				"sessions/sessions.gene.commerce-plan.map.v1",
				"control/control.v1.query",
				"control/control.v1.config.validate",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				switch target + "/" + function {
				case "sessions/sessions.gene.route-intent.parse.v1":
					return fleetWire(map[string]any{
						"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
						"route": "config", "order_id": "order-1", "claim_token": "route-token-1", "server_id": "server-1",
						"request_id": "request-1", "now_unix": int64(1_750_000_000), "now_rfc3339": "2025-06-15T15:06:40Z",
					})
				case "identity/identity.route-principal.resolve.v1":
					if !test.identityOK {
						return fleetWire(map[string]any{"ok": false, "error": map[string]any{"code": "not_found", "message": "route principal token not found"}})
					}
					return fleetWire(fleetPrincipalResult())
				case "commerce/commerce.public.order-projection.get.v1":
					return fleetWire(fleetCommerceResult())
				case "commerce/commerce.public.order-projection.list.v1":
					return fleetWire(map[string]any{"ok": true, "value": []any{fleetCommercePlan()}})
				case "sessions/sessions.gene.commerce-plan.map.v1":
					return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
				case "fleet/fleet.v1.query.authorization.context":
					return fleetWire(fleetAuthorizationContext())
				case "control/control.v1.query":
					return fleetWire(fleetControlProjection())
				case "control/control.v1.config.validate":
					if !test.configOK {
						return fleetWire(map[string]any{"version": "sessions.control/v1", "valid": false, "http_status": int64(400), "http_error": "invalid config"})
					}
					return fleetWire(fleetValidConfig())
				default:
					return nil, fmt.Errorf("forbidden call after %s failure: %s/%s", test.name, target, function)
				}
			})
			defer runtime.Close()

			// A fail-closed Lua route may return a legacy HTTP error response or
			// terminate the saga with an error. Either is safe; the assertion is
			// that it never reaches a Fleet mutation.
			_ = executeFleetPublicEventResult(t, runtime, "evolution.sessions.voucher.config.v1", fleetHTTPRequest("POST", "/api/voucher/order-1/config", map[string]any{}, map[string]any{"token": "route-token-1"}))
			assertFleetCalls(t, calls, test.wantCalls...)
		})
	}
}

func newFleetPublicRuntime(t *testing.T, calls *[]string, handler func(string, string, []byte) ([]byte, error)) *Runtime {
	t.Helper()
	runtime, err := New(Options{Script: evolutionLua(t), Caller: CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		*calls = append(*calls, target+"/"+function)
		return handler(target, function, payload)
	})})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runtime
}

func executeFleetPublicEvent(t *testing.T, runtime *Runtime, event string, request map[string]any) {
	t.Helper()
	if err := executeFleetPublicEventResult(t, runtime, event, request); err != nil {
		t.Fatalf("ExecuteSaga(%s): %v", event, err)
	}
}

func executeFleetPublicEventResult(t *testing.T, runtime *Runtime, event string, request map[string]any) error {
	t.Helper()
	wire, err := msgpack.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Dispatch(workflow.DispatchRequest{
		Event: event,
		Payload: map[string]any{
			"request_msgpack": string(wire),
		},
	})
	return err
}

func fleetHTTPRequest(method, path string, body, query map[string]any) map[string]any {
	return map[string]any{"method": method, "path": path, "body": body, "query": query, "params": map[string]any{"id": "order-1"}, "client_ip": "203.0.113.8"}
}

func assertFleetRouteRequest(t *testing.T, payload []byte, method, path string) {
	t.Helper()
	var request map[string]any
	if err := msgpack.Unmarshal(payload, &request); err != nil {
		t.Fatalf("decode route request: %v", err)
	}
	if request["method"] != method || request["path"] != path {
		t.Fatalf("route request = %#v, want %s %s", request, method, path)
	}
}

func assertFleetField(t *testing.T, payload []byte, field, want string) {
	t.Helper()
	var value map[string]any
	if err := msgpack.Unmarshal(payload, &value); err != nil {
		t.Fatal(err)
	}
	if value[field] != want {
		t.Fatalf("%s = %#v, want %q in %#v", field, value[field], want, value)
	}
}

func assertFleetCalls(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("calls = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("calls = %#v, want %#v", got, want)
		}
	}
}

func fleetWire(value any) ([]byte, error) { return msgpack.Marshal(value) }

func fleetPrincipalResult() map[string]any {
	return map[string]any{"ok": true, "value": map[string]any{
		"version": "identity.route-principal.v1", "subject_id": "subject-1", "email": "player@example.test",
		"email_verified": true, "purpose": "login", "expires_at": int64(1_750_003_600_000),
	}}
}

func fleetCommercePlan() map[string]any {
	return map[string]any{
		"order":                    map[string]any{"id": "order-1", "customer_id": "subject-1", "game_id": "minecraft", "tier_id": "tier-1", "status": "paid", "config": map[string]any{}},
		"normalized_contact_email": "player@example.test", "server_type": "paper", "max_amount_cents": int64(1200),
	}
}

func fleetCommerceResult() map[string]any {
	return map[string]any{"ok": true, "value": fleetCommercePlan()}
}

func fleetMappedPlan() map[string]any {
	return map[string]any{
		"order_id": "order-1", "status": "purchased", "server_type": "paper",
		"tier_id": "tier-1", "config": map[string]any{},
	}
}

func fleetControlProjection() map[string]any {
	return map[string]any{
		"version": "sessions.control/v1",
		"visibility": []any{map[string]any{
			"game_id": "minecraft", "template": "paper", "tier_id": "tier-1", "enabled": true,
		}},
		"tiers": []any{map[string]any{
			"id": "tier-1", "enabled": true, "price_cents": int64(1200), "currency": "usd",
		}},
	}
}

func fleetRuntimeResolution() map[string]any {
	return map[string]any{
		"version": "sessions.control/v1", "game_id": "minecraft", "template": "paper", "tier_id": "tier-1",
		"approved_image": map[string]any{"reference": "registry.example/paper@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "approval_id": "approval-1", "policy_version": "sessions-runtime-v1", "approved": true},
	}
}

func fleetAuthorizationContext() map[string]any {
	return map[string]any{
		"authorized": true, "allowed_transition": true, "order_owned": true,
		"order_id": "order-1", "server_id": "server-1",
		"config": map[string]any{}, "config_revision": "config-r1", "runtime_revision": "runtime-r1",
		"runtime": map[string]any{"server_id": "server-1", "node_id": "node-1", "template": "paper"},
	}
}

func fleetValidConfig() map[string]any {
	return map[string]any{"version": "sessions.control/v1", "operation": "deploy", "valid": true, "http_status": int64(200), "game_id": "minecraft", "template": "paper", "tier_id": "tier-1", "runtime_resolution": fleetRuntimeResolution()}
}

func fleetHTTPProjection() map[string]any {
	return map[string]any{"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"}, "body": []byte(`{}`)}
}
