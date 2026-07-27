package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
)

func reportingPayload() map[string]any {
	return map[string]any{
		"request_id": "report-request-1",
		"actor":      map[string]any{"id": "admin-1", "is_admin": true},
	}
}

func reportingHTTP(body string) ([]byte, error) {
	return fleetWire(map[string]any{"ok": true, "value": map[string]any{
		"status": int64(200), "headers": map[string]any{"Content-Type": "application/json"},
		"body": []byte(body),
	}})
}

func TestReportingOrdersCSVCollectsExactIdentityAndFleetFacts(t *testing.T) {
	calls := []string{}
	exportCalls := 0
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.report.order-csv.export.v1":
			exportCalls++
			request := decodeAdminPaymentPayload(t, payload)
			if exportCalls == 1 {
				if request["composition"] != nil {
					t.Fatalf("initial order export composition = %#v", request)
				}
				return fleetWire(map[string]any{"ok": true, "value": map[string]any{
					"rows": []any{map[string]any{"id": "order-1"}}, "csv": "initial",
				}})
			}
			composition := request["composition"].([]any)
			row := composition[0].(map[string]any)
			if row["order_id"] != "order-1" || row["extend_server_id"] != "server-original" ||
				row["eu_waiver_accepted_at_unix"] != int64(1_750_000_000) {
				t.Fatalf("final order export composition = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"rows": []any{map[string]any{"id": "order-1"}}, "csv": "id,email\norder-1,player@example.test\n",
			}})
		case "identity/identity.report.eu-waiver-accepted-at.by-order.v1":
			request := decodeAdminPaymentPayload(t, payload)
			ids := request["order_ids"].([]any)
			if len(ids) != 1 || ids[0] != "order-1" {
				t.Fatalf("EU waiver query = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": []any{
				map[string]any{"order_id": "order-1", "eu_waiver_accepted_at_unix": int64(1_750_000_000)},
			}})
		case "fleet/fleet.v1.query.server.list":
			return fleetWire([]any{map[string]any{
				"id": "server-extension", "order_id": "order-1", "extends_server_id": "server-original",
			}})
		case "commerce/commerce.admin.http.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			result := request["result"].(map[string]any)
			if request["operation"] != "order_export_csv" || result["value"] != "id,email\norder-1,player@example.test\n" {
				t.Fatalf("order CSV projection = %#v", request)
			}
			return reportingHTTP("id,email\norder-1,player@example.test\n")
		default:
			return nil, fmt.Errorf("unexpected order reporting call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	payload := reportingPayload()
	payload["download_filename"] = "orders-2026-07-25.csv"
	if _, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.reporting.orders-csv.v1", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.report.order-csv.export.v1",
		"identity/identity.report.eu-waiver-accepted-at.by-order.v1",
		"fleet/fleet.v1.query.server.list",
		"commerce/commerce.report.order-csv.export.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}

func TestReportingUsersCSVCollectsOrderEmailFactsBeforeFleetCounts(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.report.order-email-facts.list.v1":
			return fleetWire(map[string]any{"ok": true, "value": []any{
				map[string]any{"order_id": "order-1", "email": "player@example.test"},
			}})
		case "fleet/fleet.v1.report.active-servers.by-email":
			request := decodeAdminPaymentPayload(t, payload)
			orders := request["orders"].([]any)
			if len(orders) != 1 || orders[0].(map[string]any)["order_id"] != "order-1" {
				t.Fatalf("Fleet users reporting request = %#v", request)
			}
			return fleetWire([]any{map[string]any{"email": "player@example.test", "active_servers": int64(1)}})
		case "commerce/commerce.report.user-csv.export.v1":
			request := decodeAdminPaymentPayload(t, payload)
			composition := request["composition"].([]any)
			if len(composition) != 1 || composition[0].(map[string]any)["active_servers"] != int64(1) {
				t.Fatalf("users CSV composition = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{"csv": "email,active_servers\nplayer@example.test,1\n"}})
		case "commerce/commerce.admin.http.project.v1":
			return reportingHTTP("email,active_servers\nplayer@example.test,1\n")
		default:
			return nil, fmt.Errorf("unexpected users reporting call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	payload := reportingPayload()
	payload["download_filename"] = "users-2026-07-25.csv"
	if _, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.reporting.users-csv.v1", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.report.order-email-facts.list.v1",
		"fleet/fleet.v1.report.active-servers.by-email",
		"commerce/commerce.report.user-csv.export.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}

func TestReportingAnalyticsCollectsControlAndFleetFacts(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "control/control.report.tier-labels.get.v1":
			request := decodeAdminPaymentPayload(t, payload)
			if request["version"] != "sessions.control/v1" {
				t.Fatalf("Control reporting request = %#v", request)
			}
			return fleetWire(map[string]any{
				"version": "sessions.control/v1", "revision": int64(3),
				"tier_labels": map[string]any{"starter": "Starter"},
			})
		case "fleet/fleet.v1.report.server-state-counts":
			return fleetWire(map[string]any{"counts": map[string]any{"active": int64(2)}})
		case "commerce/commerce.report.analytics.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			composition := request["composition"].(map[string]any)
			if composition["tier_labels"].(map[string]any)["starter"] != "Starter" ||
				composition["server_state_counts"].(map[string]any)["active"] != int64(2) {
				t.Fatalf("analytics composition = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{"generated_at": "2026-07-25T12:00:00Z"}})
		case "commerce/commerce.admin.http.project.v1":
			return reportingHTTP(`{"generated_at":"2026-07-25T12:00:00Z"}`)
		default:
			return nil, fmt.Errorf("unexpected analytics reporting call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	payload := reportingPayload()
	payload["now_unix"] = int64(1_753_444_800)
	if _, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.reporting.analytics.v1", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"control/control.report.tier-labels.get.v1",
		"fleet/fleet.v1.report.server-state-counts",
		"commerce/commerce.report.analytics.project.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}

func TestReportingDisputeComposesAllExactOwnerEvidence(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		switch target + "/" + function {
		case "commerce/commerce.report.dispute.commerce-read.v1":
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"email": "player@example.test", "generated_at": "2026-07-25T12:00:00Z",
				"orders": []any{map[string]any{"id": "order-1", "email": "player@example.test"}},
				"stripe_events": []any{},
			}})
		case "fleet/fleet.v1.query.server.list":
			return fleetWire([]any{map[string]any{
				"id": "server-1", "order_id": "order-1", "status": "active",
				"created_at": "2026-07-20T12:00:00Z", "expires_at": "2026-08-20T12:00:00Z",
			}})
		case "funding/funding.report.pool-contributions.by-email.v1":
			return fleetWire(map[string]any{"ok": true, "value": []any{
				map[string]any{"id": "contribution-1", "amount_cents": int64(500)},
			}})
		case "identity/identity.report.deletion-requests.by-email.v1":
			return fleetWire(map[string]any{"ok": true, "value": []any{
				map[string]any{"id": "deletion-1", "created_at": "2026-07-21T12:00:00Z"},
			}})
		case "fleet/fleet.v1.report.reconfigurations.by-order":
			return fleetWire([]any{map[string]any{"id": "reconfigure-1", "order_id": "order-1"}})
		case "fleet/fleet.v1.report.connection-logs.by-order":
			return fleetWire([]any{map[string]any{"id": "connection-1", "order_id": "order-1"}})
		case "commerce/commerce.report.dispute.project.v1":
			request := decodeAdminPaymentPayload(t, payload)
			commerce := request["commerce"].(map[string]any)
			order := commerce["orders"].([]any)[0].(map[string]any)
			server := order["server"].(map[string]any)
			if server["id"] != "server-1" || server["state"] != "active" ||
				len(request["pool_contributions"].([]any)) != 1 ||
				len(request["deletion_requests"].([]any)) != 1 ||
				len(request["reconfigurations"].([]any)) != 1 ||
				len(request["connection_logs"].([]any)) != 1 {
				t.Fatalf("dispute composition = %#v", request)
			}
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{"email": "player@example.test", "total_orders": int64(1)}})
		case "commerce/commerce.admin.http.project.v1":
			return reportingHTTP(`{"email":"player@example.test","total_orders":1}`)
		default:
			return nil, fmt.Errorf("unexpected dispute reporting call %s/%s", target, function)
		}
	})
	defer runtime.Close()

	payload := reportingPayload()
	payload["query"] = map[string]any{"email": "player@example.test"}
	payload["generated_at_unix"] = int64(1_753_444_800)
	payload["inline"] = true
	if _, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.commerce.admin.reporting.dispute-report.v1", Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	assertFleetCalls(t, calls,
		"commerce/commerce.report.dispute.commerce-read.v1",
		"fleet/fleet.v1.query.server.list",
		"funding/funding.report.pool-contributions.by-email.v1",
		"identity/identity.report.deletion-requests.by-email.v1",
		"fleet/fleet.v1.report.reconfigurations.by-order",
		"fleet/fleet.v1.report.connection-logs.by-order",
		"commerce/commerce.report.dispute.project.v1",
		"commerce/commerce.admin.http.project.v1",
	)
}
