package orchestrator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// These tests pin the public read composition independently from the mutation
// matrix. A read may not use the legacy aggregate Gene handler: each fact must
// come from its owner, and the stateless response projector owns HTTP shaping.

func TestFleetPublicSessionsReadMatrixUsesVerifiedIdentityAndPublicCommerceProof(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "sessions/sessions.gene.route-intent.parse.v1":
			return fleetWire(map[string]any{
				"http_status": int64(200), "route": "sessions", "claim_token": "claim-1",
				"request_id": "request-1", "now_unix": int64(1_750_000_000), "now_rfc3339": "2025-06-15T15:06:40Z",
			})
		case "identity/identity.route-principal.resolve.v1":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["version"] != "identity.route-principal.v1" || request["token"] != "claim-1" || request["purpose"] != "login" || request["now"] != int64(1_750_000_000_000) {
				t.Fatalf("identity request = %#v", request)
			}
			return fleetWire(fleetPrincipalResult())
		case "commerce/commerce.public.order-projection.list.v1":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			proof, ok := request["proof"].(map[string]any)
			if !ok || request["subject_id"] != "subject-1" || request["limit"] != int64(100) ||
				proof["subject_id"] != "subject-1" || proof["normalized_email"] != "player@example.test" ||
				proof["proof_id"] != "claim:claim-1" || proof["verified_at_unix"] != int64(1_750_000_000) || proof["authorized"] != true {
				t.Fatalf("Commerce proof request = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": []any{fleetCommercePlan()}})
		case "sessions/sessions.gene.commerce-plan.map.v1":
			return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
		case "fleet/fleet.v1.query.gene.sessions":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			plans, ok := request["plans"].([]any)
			if !ok || len(plans) != 1 || request["proven"] != true {
				t.Fatalf("Fleet sessions request = %#v", request)
			}
			return fleetWire(map[string]any{"sessions": []any{}})
		case "sessions/sessions.gene.fleet-response.project.v1":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["route"] != "sessions" {
				t.Fatalf("response projection = %#v", request)
			}
			return fleetWire(fleetHTTPProjection())
		default:
			return nil, fmt.Errorf("unexpected sessions read call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	executeFleetPublicEvent(t, runtime, "evolution.sessions.sessions.get.v1", fleetHTTPRequest("GET", "/api/sessions", nil, map[string]any{"token": "claim-1"}))
	assertFleetCalls(t, calls,
		"sessions/sessions.gene.route-intent.parse.v1",
		"identity/identity.route-principal.resolve.v1",
		"commerce/commerce.public.order-projection.list.v1",
		"sessions/sessions.gene.commerce-plan.map.v1",
		"fleet/fleet.v1.query.gene.sessions",
		"sessions/sessions.gene.fleet-response.project.v1",
	)
}

func TestFleetPublicAvailabilityReadMatrixUsesGeneCommerceControlAndProjector(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "sessions/sessions.gene.route-intent.parse.v1":
			return fleetWire(map[string]any{"http_status": int64(200), "route": "availability", "order_id": "order-1", "template": "paper"})
		case "commerce/commerce.gene.order-projection.get.v1":
			assertFleetField(t, payload, "order_id", "order-1")
			return fleetWire(fleetCommerceResult())
		case "sessions/sessions.gene.commerce-plan.map.v1":
			return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
		case "control/control.v1.query":
			return fleetWire(fleetControlProjection())
		case "control/control.v1.config.validate":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["operation"] != "deploy" || request["template"] != "paper" || request["tier_id"] != "tier-1" || request["game_id"] != "minecraft" {
				t.Fatalf("Control validation request = %#v", request)
			}
			return fleetWire(fleetValidConfig())
		case "fleet/fleet.v1.query.gene.availability":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["plan"] == nil || request["template"] != "paper" {
				t.Fatalf("Fleet availability request = %#v", request)
			}
			return fleetWire(map[string]any{"available": true})
		case "sessions/sessions.gene.fleet-response.project.v1":
			var request map[string]any
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request["route"] != "availability" {
				t.Fatalf("response projection = %#v", request)
			}
			return fleetWire(fleetHTTPProjection())
		default:
			return nil, fmt.Errorf("unexpected availability read call %s/%s", target, function)
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

func TestFleetPublicReadsFailClosedBeforeFleet(t *testing.T) {
	t.Run("sessions-unverified-principal", func(t *testing.T) {
		calls := []string{}
		runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
			switch target + "/" + function {
			case "sessions/sessions.gene.route-intent.parse.v1":
				return fleetWire(map[string]any{"http_status": int64(200), "route": "sessions", "claim_token": "claim-1", "now_unix": int64(1)})
			case "identity/identity.route-principal.resolve.v1":
				return fleetWire(map[string]any{"ok": true, "value": map[string]any{"email_verified": false}})
			default:
				return nil, fmt.Errorf("unsafe call after identity rejection: %s/%s", target, function)
			}
		})
		defer runtime.Close()
		_ = executeFleetPublicEventResult(t, runtime, "evolution.sessions.sessions.get.v1", fleetHTTPRequest("GET", "/api/sessions", nil, map[string]any{"token": "claim-1"}))
		assertFleetCalls(t, calls,
			"sessions/sessions.gene.route-intent.parse.v1",
			"identity/identity.route-principal.resolve.v1",
		)
	})

	t.Run("availability-missing-commerce-projection", func(t *testing.T) {
		calls := []string{}
		runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
			switch target + "/" + function {
			case "sessions/sessions.gene.route-intent.parse.v1":
				return fleetWire(map[string]any{"http_status": int64(200), "route": "availability", "order_id": "missing"})
			case "commerce/commerce.gene.order-projection.get.v1":
				return fleetWire(map[string]any{"ok": false, "error": map[string]any{"code": "not_found"}})
			default:
				return nil, fmt.Errorf("unsafe call after Commerce rejection: %s/%s", target, function)
			}
		})
		defer runtime.Close()
		_ = executeFleetPublicEventResult(t, runtime, "evolution.sessions.voucher.availability.get.v1", fleetHTTPRequest("GET", "/api/voucher/missing/availability", nil, nil))
		assertFleetCalls(t, calls,
			"sessions/sessions.gene.route-intent.parse.v1",
			"commerce/commerce.gene.order-projection.get.v1",
		)
	})
}

func TestFleetPublicReadsGeneratedLuaHasNoLegacyAggregateRouteProvider(t *testing.T) {
	lua := evolutionLua(t)
	if strings.Contains(lua, "gene.handle_route") {
		t.Fatal("generated Sessions Lua must not call the legacy aggregate gene.handle_route provider")
	}
	for _, event := range []string{
		"evolution.sessions.sessions.get.v1",
		"evolution.sessions.voucher.availability.get.v1",
		"sessions.gene.fleet-response.project.v1",
	} {
		if !strings.Contains(lua, event) {
			t.Fatalf("generated Sessions Lua is missing public read composition marker %q", event)
		}
	}
}
