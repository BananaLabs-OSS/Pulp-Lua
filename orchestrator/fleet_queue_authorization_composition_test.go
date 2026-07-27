package orchestrator

import (
	"fmt"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFleetQueueCommandsFetchCommerceAuthorizationFact(t *testing.T) {
	tests := []struct {
		name     string
		event    string
		provider string
		request  map[string]any
		wantID   string
	}{
		{
			name: "ensure", event: "fleet.workflow.queue.server.ensure.v1",
			provider: "fleet.v1.command.queue.server.ensure",
			request: map[string]any{
				"server": map[string]any{"id": "server-1", "order_id": "order-1", "template": "paper"},
				"ticket": map[string]any{"id": "ticket-1", "server_id": "server-1"},
			},
			wantID: "fleet-queue-ensure:commerce-fact-a:7:ticket-1",
		},
		{
			name: "promote", event: "fleet.workflow.queue.provision.promote.v1",
			provider: "fleet.v1.command.queue.provision.promote",
			request: map[string]any{
				"order_id": "order-1", "server_id": "server-1", "ticket_id": "ticket-1",
				"admission_key": "admission-1", "cpu": int64(2), "memory": int64(4096),
			},
			wantID: "fleet-queue-promote:commerce-fact-a:7:ticket-1:admission-1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
				switch target + "/" + function {
				case "commerce/commerce.queue.authorization-fact.get.v1":
					assertFleetField(t, payload, "order_id", "order-1")
					assertFleetField(t, payload, "server_id", "server-1")
					return fleetWire(map[string]any{"ok": true, "value": map[string]any{
						"fact_id": "commerce-fact-a", "revision": int64(7), "order_id": "order-1",
						"server_id": "server-1", "fulfillment_state": "paid", "payment_receipt_id": "receipt-1",
					}})
				case "fleet/" + test.provider:
					var request map[string]any
					if err := msgpack.Unmarshal(payload, &request); err != nil {
						t.Fatal(err)
					}
					if request["id"] != test.wantID {
						t.Fatalf("canonical id = %#v, want %q", request["id"], test.wantID)
					}
					commerce, ok := request["commerce"].(map[string]any)
					if !ok || commerce["fact_id"] != "commerce-fact-a" {
						t.Fatalf("Fleet request has no exact Commerce fact: %#v", request)
					}
					return fleetWire(map[string]any{"ok": true})
				default:
					return nil, fmt.Errorf("unexpected queue composition call %s/%s", target, function)
				}
			})
			defer runtime.Close()

			if _, err := runtime.Dispatch(workflow.DispatchRequest{Event: test.event, Payload: map[string]any{
				"request": test.request,
			}}); err != nil {
				t.Fatal(err)
			}
			assertFleetCalls(t, calls,
				"commerce/commerce.queue.authorization-fact.get.v1",
				"fleet/"+test.provider,
			)
		})
	}
}

func TestFleetQueueCommandRejectsCallerCanonicalIDOverride(t *testing.T) {
	calls := []string{}
	runtime := newFleetPublicRuntime(t, &calls, func(target, function string, payload []byte) ([]byte, error) {
		if target+"/"+function == "commerce/commerce.queue.authorization-fact.get.v1" {
			return fleetWire(map[string]any{"ok": true, "value": map[string]any{
				"fact_id": "commerce-fact-a", "revision": int64(7), "order_id": "order-1",
				"server_id": "server-1", "fulfillment_state": "paid",
			}})
		}
		return nil, fmt.Errorf("unexpected queue override call %s/%s", target, function)
	})
	defer runtime.Close()

	_, err := runtime.Dispatch(workflow.DispatchRequest{
		Event: "fleet.workflow.queue.server.ensure.v1",
		Payload: map[string]any{"request": map[string]any{
			"id":     "caller-chosen",
			"server": map[string]any{"id": "server-1", "order_id": "order-1", "template": "paper"},
			"ticket": map[string]any{"id": "ticket-1", "server_id": "server-1"},
		}},
	})
	if err == nil {
		t.Fatal("caller-chosen canonical id was accepted")
	}
	assertFleetCalls(t, calls, "commerce/commerce.queue.authorization-fact.get.v1")
}
