package orchestrator

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestFleetPublicMutationMatrixUsesExactOwners(t *testing.T) {
	tests := []struct {
		route   string
		event   string
		path    string
		body    map[string]any
		control bool
	}{
		{"deploy", "evolution.sessions.session.deploy.v1", "/api/session/order-1/deploy", map[string]any{"server_type": "paper"}, true},
		{"schedule", "evolution.sessions.voucher.schedule.v1", "/api/voucher/order-1/schedule", map[string]any{"date": "2026-07-26", "server_type": "paper"}, true},
		{"unschedule", "evolution.sessions.voucher.unschedule.v1", "/api/voucher/order-1/unschedule", map[string]any{}, false},
		{"swap", "evolution.sessions.voucher.swap.v1", "/api/voucher/order-1/swap", map[string]any{"target_template": "paper"}, true},
		{"upgrade", "evolution.sessions.session.upgrade.v1", "/api/session/order-1/upgrade", map[string]any{"new_template": "paper", "new_tier": "tier-1"}, true},
		{"reconfigure", "evolution.sessions.session.reconfigure.v1", "/api/session/order-1/reconfigure", map[string]any{"server_type": "paper", "motd": "hello"}, true},
	}
	for _, test := range tests {
		t.Run(test.route, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				call := target + "/" + function
				switch call {
				case "sessions/sessions.gene.route-intent.parse.v1":
					intent := map[string]any{
						"version": "sessions.gene.route-intent.parse.v1", "http_status": int64(200),
						"route": test.route, "order_id": "order-1", "claim_token": "route-token-1",
						"request_id": "request-1", "now_unix": int64(1_750_000_000),
						"now_rfc3339": "2025-06-15T15:06:40Z", "server_id": "server-1",
						"server_type": "paper", "target_template": "paper", "new_template": "paper",
						"new_tier": "tier-1", "date": "2026-07-26", "config": map[string]any{"motd": "hello"},
					}
					return fleetWire(intent)
				case "identity/identity.route-principal.resolve.v1":
					return fleetWire(fleetPrincipalResult())
				case "commerce/commerce.public.order-projection.list.v1":
					return fleetWire(map[string]any{"ok": true, "value": []any{fleetCommercePlan()}})
				case "commerce/commerce.public.order-projection.get.v1":
					return fleetWire(fleetCommerceResult())
				case "fleet/fleet.v1.query.authorization.context":
					return fleetWire(fleetAuthorizationContext())
				case "sessions/sessions.gene.commerce-plan.map.v1":
					return fleetWire(map[string]any{"ok": true, "value": fleetMappedPlan()})
				case "control/control.v1.query":
					return fleetWire(fleetControlProjection())
				case "control/control.v1.config.validate":
					return fleetWire(fleetValidConfig())
				case "sessions/sessions.gene.fleet-response.project.v1":
					return fleetWire(fleetHTTPProjection())
				}
				if target == "fleet" && strings.HasSuffix(function, ".gene."+test.route) {
					var request map[string]any
					if err := msgpack.Unmarshal(payload, &request); err != nil {
						t.Fatal(err)
					}
					if request["plan"] == nil {
						t.Fatalf("%s command lacks Commerce plan: %#v", test.route, request)
					}
					return fleetWire(fleetHTTPProjection())
				}
				return nil, fmt.Errorf("unexpected %s call %s", test.route, call)
			})
			defer runtime.Close()

			executeFleetPublicEvent(t, runtime, test.event, fleetHTTPRequest("POST", test.path, test.body, map[string]any{"token": "route-token-1"}))
			required := []string{
				"sessions/sessions.gene.route-intent.parse.v1",
				"identity/identity.route-principal.resolve.v1",
				"commerce/commerce.public.order-projection.list.v1",
				"commerce/commerce.public.order-projection.get.v1",
				"fleet/fleet.v1.query.authorization.context",
				"sessions/sessions.gene.commerce-plan.map.v1",
			}
			if test.control {
				required = append(required,
					"control/control.v1.query",
					"control/control.v1.config.validate",
				)
			}
			required = append(required,
				"fleet/fleet.v1.command.gene."+test.route,
				"sessions/sessions.gene.fleet-response.project.v1",
			)
			assertFleetCalls(t, calls, required...)
		})
	}
}
