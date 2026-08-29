package orchestrator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

// These two structs intentionally encode equivalent route DTOs in different
// key orders. Lua must validate those exact ingress bytes, then lower both
// forms to one canonical host-executor payload ABI.
type serverMutationSettingsRouteA struct {
	Authorization map[string]any `msgpack:"authorization"`
	CommandID     map[string]any `msgpack:"command_id"`
	Workload      map[string]any `msgpack:"workload"`
	Node          map[string]any `msgpack:"node"`
	Now           string         `msgpack:"now"`
	Minecraft     map[string]any `msgpack:"minecraft"`
}

type serverMutationSettingsRouteB struct {
	Minecraft     map[string]any `msgpack:"minecraft"`
	Now           string         `msgpack:"now"`
	Node          map[string]any `msgpack:"node"`
	Workload      map[string]any `msgpack:"workload"`
	CommandID     map[string]any `msgpack:"command_id"`
	Authorization map[string]any `msgpack:"authorization"`
}

type serverMutationRuntimeActionRequestV4 struct {
	Version            string `msgpack:"version"`
	ExpectedGeneration int64  `msgpack:"expected_generation"`
	Payload            []byte `msgpack:"payload"`
	PayloadSHA256      string `msgpack:"payload_sha256"`
	ID                 struct {
		Value string `msgpack:"value"`
	} `msgpack:"id"`
	Idempotency struct {
		Value string `msgpack:"value"`
	} `msgpack:"idempotency"`
}

func serverMutationHostResultV4(operation string) ([]byte, error) {
	kind, status := "exec", "executed"
	if operation == "minecraft.world.regenerate" {
		kind, status = "regenerate", "regenerated"
	}
	owner := "runtime-control"
	if operation == "minecraft.recreate" {
		kind, status, owner = "recreate", "recreating", "workload-provisioning"
	}
	output, err := msgpack.Marshal(map[string]any{
		"version": "server-mutation-runtime-result.v1",
		"kind":    kind, "status": status, "response": []byte(`{"ok":true}`),
	})
	if err != nil {
		return nil, err
	}
	outputSum := sha256.Sum256(output)
	operationReceipt, err := msgpack.Marshal(map[string]any{
		"version": "contracts.v1", "operation": operation,
		"fence": map[string]any{}, "output": output,
		"output_sha256": hex.EncodeToString(outputSum[:]),
		"completed_at":  "2026-07-26T12:00:01Z",
	})
	if err != nil {
		return nil, err
	}
	genericReceipt, err := msgpack.Marshal(map[string]any{
		"version": "contracts.v1", "succeeded": true,
		"fence": map[string]any{},
	})
	if err != nil {
		return nil, err
	}
	return msgpack.Marshal(map[string]any{
		"version": "server-mutation-host.v4", "owner": owner,
		"generic_receipt": genericReceipt, "operation_receipt": operationReceipt,
	})
}

func serverMutationWhitelistHostResultV4(added, uuid string) ([]byte, error) {
	output, err := msgpack.Marshal(map[string]any{
		"version": "server-mutation-whitelist-result.v1",
		"kind":    "whitelist.add", "status": "executed",
		"added": added, "uuid": uuid,
	})
	if err != nil {
		return nil, err
	}
	outputSum := sha256.Sum256(output)
	operationReceipt, err := msgpack.Marshal(map[string]any{
		"version": "contracts.v1", "operation": "minecraft.access.apply",
		"fence": map[string]any{}, "output": output,
		"output_sha256": hex.EncodeToString(outputSum[:]),
		"completed_at":  "2026-07-26T12:00:01Z",
	})
	if err != nil {
		return nil, err
	}
	genericReceipt, err := msgpack.Marshal(map[string]any{
		"version": "contracts.v1", "succeeded": true, "fence": map[string]any{},
	})
	if err != nil {
		return nil, err
	}
	return msgpack.Marshal(map[string]any{
		"version": "server-mutation-host.v4", "owner": "runtime-control",
		"generic_receipt": genericReceipt, "operation_receipt": operationReceipt,
	})
}

func serverMutationWorkflowsV4Lua(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "Sessions-Gene", "application", "server_mutation_workflows_v4.lua")
	script, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	// The production module leaves registration to its generated composition.
	// This native runtime test registers exactly its declared events in-memory.
	return strings.TrimSuffix(strings.TrimSpace(string(script)), "return M") + "M.register()\n"
}

func serverMutationSettingsRouteValues() (map[string]any, map[string]any, map[string]any, map[string]any, string, map[string]any) {
	opaque := func(value string) map[string]any { return map[string]any{"version": "contracts.v1", "value": value} }
	authorization := map[string]any{
		"version": "evolution.sessions.server-mutation.auth.v4", "verified": true,
		"subject": "user-1", "roles": []any{"owner"}, "request_id": "request-opaque-1",
	}
	commandID := opaque("command-1")
	workload := map[string]any{"version": "contracts.v1", "id": opaque("workload-1")}
	node := map[string]any{"version": "contracts.v1", "id": opaque("node-1")}
	minecraft := map[string]any{
		"version": "evolution.minecraft.server-mutation.v4", "kind": "settings-gamerules",
		"body": map[string]any{
			"operation": "minecraft.settings.apply", "payload": map[string]any{"difficulty": "hard"},
			"projection": map[string]any{"saved": true, "needs_restart": true, "restart_settings": []any{"difficulty"}},
		},
	}
	return authorization, commandID, workload, node, "2026-07-26T12:00:00Z", minecraft
}

func serverMutationProjectionWireV4(t *testing.T, status int64, body any) []byte {
	t.Helper()
	bodyWire, err := msgpack.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := msgpack.Marshal(map[string]any{"status": status, "body": msgpack.RawMessage(bodyWire)})
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func TestServerMutationWorkflowsV4RuntimeBuildsCanonicalHostPayloadAndRejectsTamper(t *testing.T) {
	authorization, commandID, workload, node, now, minecraft := serverMutationSettingsRouteValues()
	first, err := msgpack.Marshal(serverMutationSettingsRouteA{authorization, commandID, workload, node, now, minecraft})
	if err != nil {
		t.Fatal(err)
	}
	second, err := msgpack.Marshal(serverMutationSettingsRouteB{minecraft, now, node, workload, commandID, authorization})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("test route encodings unexpectedly have the same key order")
	}

	var accepted [][]byte
	var identities []string
	lastOperation := ""
	runtime, err := New(Options{Script: serverMutationWorkflowsV4Lua(t), Caller: CallFunc(func(target, function string, wire []byte) ([]byte, error) {
		switch target + "/" + function {
		case "configuration-registry/configuration-registry.v1.fact.get":
			return msgpack.Marshal(map[string]any{"found": false})
		case "configuration-registry/configuration-registry.v1.fact.put":
			return msgpack.Marshal(map[string]any{"registry_revision": int64(1)})
		case "runtime-control/runtime-control.v1.runtime.get":
			return msgpack.Marshal(map[string]any{
				"version": "runtime-control.v1", "workload": workload, "generation": int64(7), "revision": int64(1),
			})
		case "runtime-control/runtime-control.v1.action.request":
			var request serverMutationRuntimeActionRequestV4
			if err := msgpack.Unmarshal(wire, &request); err != nil {
				return nil, fmt.Errorf("decode runtime action request: %w", err)
			}
			if request.Version != "runtime-control.v1" || request.ExpectedGeneration != 7 {
				return nil, fmt.Errorf("runtime action fence = %#v", request)
			}
			sum := sha256.Sum256(request.Payload)
			if request.PayloadSHA256 != hex.EncodeToString(sum[:]) {
				return nil, fmt.Errorf("opaque payload digest does not match exact bytes")
			}
			var envelope map[string]any
			if err := msgpack.Unmarshal(request.Payload, &envelope); err != nil {
				return nil, fmt.Errorf("decode canonical host envelope: %w", err)
			}
			body, ok := envelope["body"].(map[string]any)
			if !ok || envelope["version"] != "evolution.minecraft.server-mutation.v4" ||
				envelope["kind"] != "settings-gamerules" ||
				body["operation"] != "minecraft.settings.apply" ||
				body["expected_generation"] != int64(7) ||
				body["workload"] == nil || body["node"] == nil || body["payload"] == nil ||
				body["projection"] != nil || envelope["authorization"] != nil {
				return nil, fmt.Errorf("canonical host envelope = %#v", envelope)
			}
			accepted = append(accepted, append([]byte(nil), request.Payload...))
			identities = append(identities, request.ID.Value+"|"+request.Idempotency.Value)
			lastOperation, _ = body["operation"].(string)
			return msgpack.Marshal(map[string]any{
				"version": "runtime-control.v1",
				"intent":  map[string]any{"id": map[string]any{"version": "contracts.v1", "value": request.ID.Value}},
			})
		case "runtime-control/runtime-control.v1.effect.claim.exact":
			return msgpack.Marshal(map[string]any{
				"intent": map[string]any{"id": map[string]any{"version": "contracts.v1", "value": "intent-1"}},
				"fence":  map[string]any{}, "lease_expires_at": "2026-07-26T12:02:00Z",
			})
		case "effects/server-mutation.host.execute.v4":
			return serverMutationHostResultV4(lastOperation)
		case "runtime-control/runtime-control.v1.effect.receipt.apply":
			return msgpack.Marshal(map[string]any{"version": "runtime-control.v1", "status": "succeeded"})
		default:
			return nil, fmt.Errorf("unexpected owner call %s/%s", target, function)
		}
	})})
	if err != nil {
		t.Fatalf("New real Pulp-Lua runtime: %v", err)
	}
	defer runtime.Close()

	projectionBody := map[string]any{"saved": true, "needs_restart": true, "restart_settings": []any{"difficulty"}}
	projection := serverMutationProjectionWireV4(t, 200, projectionBody)
	dispatch := func(route []byte, digest string, projection []byte, projectionDigest string) (workflow.DispatchResult, error) {
		return runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.server.mutation.v4", Payload: map[string]any{
			"version": "evolution.server-mutation-owner-adapter.v4", "operation": "minecraft.settings.apply",
			"payload": route, "payload_sha256": digest, "projection": projection, "projection_sha256": projectionDigest,
		}})
	}
	for _, route := range [][]byte{first, second, first, second} {
		sum := sha256.Sum256(route)
		projectionSum := sha256.Sum256(projection)
		result, err := dispatch(route, hex.EncodeToString(sum[:]), projection, hex.EncodeToString(projectionSum[:]))
		if err != nil {
			t.Fatalf("real runtime dispatch: %v", err)
		}
		projection, ok := result.Value.(map[string]any)
		body, bodyOK := projection["body"].(map[string]any)
		if !ok || projection["status"] != int64(200) || !bodyOK || body["saved"] != true || body["difficulty"] != nil {
			t.Fatalf("legacy settings projection = %#v", result.Value)
		}
	}
	if len(accepted) != 4 {
		t.Fatalf("accepted actions = %d", len(accepted))
	}
	for index := range accepted {
		if !bytes.Equal(accepted[index], accepted[0]) {
			t.Fatalf("action %d changed canonical host bytes\nfirst: %x\n got: %x", index, accepted[0], accepted[index])
		}
	}
	if bytes.Equal(accepted[0], first) || bytes.Equal(accepted[0], second) {
		t.Fatal("canonical host payload retained the ingress route envelope")
	}
	projectionSum := sha256.Sum256(projection)
	for _, identity := range identities {
		if !strings.Contains(identity, ":projection:"+hex.EncodeToString(projectionSum[:])) {
			t.Fatalf("projection digest is not bound to owner identity: %q", identity)
		}
	}

	if _, err := dispatch(first, strings.Repeat("0", 64), projection, hex.EncodeToString(projectionSum[:])); err == nil || !strings.Contains(err.Error(), "AppCall payload digest does not match exact bytes") {
		t.Fatalf("tampered digest was accepted: %v", err)
	}
	// An injected projection field and a cross-operation queued projection both
	// fail in Lua before reaching runtime-control.
	rawProjectionBody, err := msgpack.Marshal(projectionBody)
	if err != nil {
		t.Fatal(err)
	}
	injected, err := msgpack.Marshal(map[string]any{"status": int64(200), "body": msgpack.RawMessage(rawProjectionBody), "headers": map[string]any{"X-Injected": "no"}})
	if err != nil {
		t.Fatal(err)
	}
	injectedSum := sha256.Sum256(injected)
	payloadSum := sha256.Sum256(first)
	if _, err := dispatch(first, hex.EncodeToString(payloadSum[:]), injected, hex.EncodeToString(injectedSum[:])); err == nil || !strings.Contains(err.Error(), "legacy projection has unsupported field headers") {
		t.Fatalf("injected projection was accepted: %v", err)
	}
	crossOperation, err := msgpack.Marshal(map[string]any{"status": int64(202), "body": map[string]any{"status": "backing_up"}})
	if err != nil {
		t.Fatal(err)
	}
	crossSum := sha256.Sum256(crossOperation)
	if _, err := dispatch(first, hex.EncodeToString(payloadSum[:]), crossOperation, hex.EncodeToString(crossSum[:])); err == nil || !strings.Contains(err.Error(), "legacy projection status is not approved") {
		t.Fatalf("cross-operation projection was accepted: %v", err)
	}
	missingProjectionMinecraft := map[string]any{
		"version": "evolution.minecraft.server-mutation.v4", "kind": "settings-gamerules",
		"body": map[string]any{"operation": "minecraft.settings.apply", "payload": map[string]any{"difficulty": "hard"}},
	}
	missingProjectionRoute, err := msgpack.Marshal(serverMutationSettingsRouteA{authorization, commandID, workload, node, now, missingProjectionMinecraft})
	if err != nil {
		t.Fatal(err)
	}
	missingSum := sha256.Sum256(missingProjectionRoute)
	if _, err := dispatch(missingProjectionRoute, hex.EncodeToString(missingSum[:]), projection, hex.EncodeToString(projectionSum[:])); err == nil || !strings.Contains(err.Error(), "minecraft.body.projection is required") {
		t.Fatalf("route without response-only projection was accepted: %v", err)
	}
}

func TestServerMutationWorkflowsV4RuntimeProjectionParityAcrossSupportedOperations(t *testing.T) {
	authorization, _, workload, node, now, _ := serverMutationSettingsRouteValues()
	opaque := func(value string) map[string]any { return map[string]any{"version": "contracts.v1", "value": value} }
	lastOperation := ""
	runtime, err := New(Options{Script: serverMutationWorkflowsV4Lua(t), Caller: CallFunc(func(target, function string, wire []byte) ([]byte, error) {
		switch target + "/" + function {
		case "configuration-registry/configuration-registry.v1.fact.get":
			return msgpack.Marshal(map[string]any{"found": false})
		case "configuration-registry/configuration-registry.v1.fact.put":
			return msgpack.Marshal(map[string]any{"registry_revision": int64(1)})
		case "runtime-control/runtime-control.v1.runtime.get":
			return msgpack.Marshal(map[string]any{"version": "runtime-control.v1", "workload": workload, "generation": int64(7), "revision": int64(1)})
		case "runtime-control/runtime-control.v1.action.request":
			var request serverMutationRuntimeActionRequestV4
			if err := msgpack.Unmarshal(wire, &request); err != nil {
				return nil, err
			}
			if request.ID.Value == "" || request.Idempotency.Value == "" || len(request.Payload) == 0 {
				return nil, fmt.Errorf("runtime action did not carry bound command identity")
			}
			var envelope map[string]any
			if err := msgpack.Unmarshal(request.Payload, &envelope); err != nil {
				return nil, err
			}
			body, _ := envelope["body"].(map[string]any)
			lastOperation, _ = body["operation"].(string)
			return msgpack.Marshal(map[string]any{
				"version": "runtime-control.v1",
				"intent":  map[string]any{"id": map[string]any{"version": "contracts.v1", "value": request.ID.Value}},
			})
		case "runtime-control/runtime-control.v1.effect.claim.exact":
			return msgpack.Marshal(map[string]any{
				"intent": map[string]any{"id": map[string]any{"version": "contracts.v1", "value": "intent-1"}},
				"fence":  map[string]any{}, "lease_expires_at": "2026-07-26T12:02:00Z",
			})
		case "effects/server-mutation.host.execute.v4":
			return serverMutationHostResultV4(lastOperation)
		case "runtime-control/runtime-control.v1.effect.receipt.apply":
			return msgpack.Marshal(map[string]any{"version": "runtime-control.v1", "status": "succeeded"})
		case "workload-provisioning/workload-provisioning.v1.get":
			return msgpack.Marshal(map[string]any{"version": "workload-provisioning.v1", "workload": workload, "generation": int64(3)})
		case "workload-provisioning/workload-provisioning.v1.reconfigure":
			lastOperation = "minecraft.recreate"
			return msgpack.Marshal(map[string]any{
				"version": "workload-provisioning.v1",
				"effect": map[string]any{
					"intent": map[string]any{
						"id": map[string]any{"version": "contracts.v1", "value": "recreate-intent-1"},
					},
				},
			})
		case "workload-provisioning/workload-provisioning.v1.effect.claim.exact":
			return msgpack.Marshal(map[string]any{
				"version": "workload-provisioning.v1", "kind": "effect.claim",
				"effect": map[string]any{
					"intent": map[string]any{
						"id": map[string]any{"version": "contracts.v1", "value": "recreate-intent-1"},
					},
				},
				"fence": map[string]any{}, "snapshot": nil,
			})
		case "workload-provisioning/workload-provisioning.v1.effect.receipt.apply":
			return msgpack.Marshal(map[string]any{
				"version": "workload-provisioning.v1", "kind": "effect.receipt.apply",
			})
		default:
			return nil, fmt.Errorf("unexpected owner call %s/%s", target, function)
		}
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	dispatch := func(operation string, route, projection []byte) (workflow.DispatchResult, error) {
		payloadSum, projectionSum := sha256.Sum256(route), sha256.Sum256(projection)
		return runtime.Dispatch(workflow.DispatchRequest{Event: "evolution.sessions.server.mutation.v4", Payload: map[string]any{
			"version": "evolution.server-mutation-owner-adapter.v4", "operation": operation,
			"payload": route, "payload_sha256": hex.EncodeToString(payloadSum[:]),
			"projection": projection, "projection_sha256": hex.EncodeToString(projectionSum[:]),
		}})
	}
	route := func(operation, kind, command string, payload, projection any) []byte {
		wire, err := msgpack.Marshal(map[string]any{
			"authorization": authorization, "command_id": opaque(command), "idempotency": opaque("idem-" + command),
			"workload": workload, "node": node, "now": now,
			"minecraft": map[string]any{"version": "evolution.minecraft.server-mutation.v4", "kind": kind, "body": map[string]any{"operation": operation, "payload": payload, "projection": projection}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	for _, test := range []struct {
		operation, kind, command string
		payload, projection      map[string]any
		status                   int64
	}{
		{"minecraft.settings.apply", "settings-gamerules", "settings", map[string]any{"difficulty": "hard"}, map[string]any{"saved": true, "needs_restart": true, "restart_settings": []any{"difficulty"}}, 200},
		{"minecraft.gamerules.apply", "settings-gamerules", "gamerules", map[string]any{"keepInventory": true}, map[string]any{"saved": true, "needs_restart": false, "restart_settings": []any{"keepInventory"}}, 200},
		{"minecraft.access.apply", "access", "access", map[string]any{"action": "allow", "uuid": "user-2"}, map[string]any{"added": true, "uuid": "user-2"}, 200},
		{"minecraft.player.command", "player", "player", map[string]any{"command": "say hello"}, map[string]any{"result": "queued"}, 200},
		{"minecraft.world.regenerate", "world", "regenerate", map[string]any{"requested": "regenerate"}, map[string]any{"status": "regenerating"}, 200},
		{"minecraft.world.backup", "world", "backup", map[string]any{"requested": "backup"}, map[string]any{"status": "backing_up"}, 202},
	} {
		t.Run(test.operation, func(t *testing.T) {
			result, err := dispatch(test.operation, route(test.operation, test.kind, test.command, test.payload, test.projection), serverMutationProjectionWireV4(t, test.status, test.projection))
			if err != nil {
				t.Fatal(err)
			}
			value, ok := result.Value.(map[string]any)
			if !ok || value["status"] != test.status || !reflect.DeepEqual(value["body"], test.projection) {
				t.Fatalf("projection = %#v, want status=%d body=%#v", result.Value, test.status, test.projection)
			}
		})
	}

	recreateRoute, err := msgpack.Marshal(map[string]any{
		"authorization": authorization, "command_id": opaque("recreate"), "idempotency": opaque("idem-recreate"),
		"workload": workload, "node": node, "now": now, "template": opaque("paper"),
		"resource_profile": opaque("standard"), "resources": map[string]any{},
		"minecraft": map[string]any{"version": "evolution.minecraft.server-mutation.v4", "kind": "recreate", "body": map[string]any{
			"operation": "minecraft.recreate", "payload": map[string]any{"requested": "recreate"}, "projection": map[string]any{"status": "recreating"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	recreateBody := map[string]any{"status": "recreating"}
	result, err := dispatch("minecraft.recreate", recreateRoute, serverMutationProjectionWireV4(t, 200, recreateBody))
	if err != nil {
		t.Fatal(err)
	}
	value, ok := result.Value.(map[string]any)
	if !ok || value["status"] != int64(200) || !reflect.DeepEqual(value["body"], recreateBody) {
		t.Fatalf("recreate projection = %#v", result.Value)
	}
	if _, err := dispatch("minecraft.recreate", recreateRoute, serverMutationProjectionWireV4(t, 200, map[string]any{"status": "tampered"})); err == nil || !strings.Contains(err.Error(), "legacy projection body is not approved") {
		t.Fatalf("tampered fixed projection was accepted: %v", err)
	}
}

func TestServerMutationWorkflowsV4RuntimeReplaysDurableOperationReceiptWithoutHostReexecution(t *testing.T) {
	authorization, commandID, workload, node, now, minecraft := serverMutationSettingsRouteValues()
	route, err := msgpack.Marshal(serverMutationSettingsRouteA{
		authorization, commandID, workload, node, now, minecraft,
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := serverMutationProjectionWireV4(
		t, 200,
		map[string]any{"saved": true, "needs_restart": true, "restart_settings": []any{"difficulty"}},
	)
	var storedKey string
	var storedPayload []byte
	var actionCalls, claimCalls, hostCalls, puts, applies int
	lastOperation := ""
	runtime, err := New(Options{
		Script: serverMutationWorkflowsV4Lua(t),
		Caller: CallFunc(func(target, function string, wire []byte) ([]byte, error) {
			switch target + "/" + function {
			case "configuration-registry/configuration-registry.v1.fact.get":
				var request struct {
					Key string `msgpack:"key"`
				}
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				if storedKey == request.Key && len(storedPayload) != 0 {
					return msgpack.Marshal(map[string]any{
						"found": true,
						"fact": map[string]any{
							"family": "config", "key": storedKey,
							"payload": storedPayload, "revision": int64(1),
						},
					})
				}
				return msgpack.Marshal(map[string]any{"found": false})
			case "configuration-registry/configuration-registry.v1.fact.put":
				var request struct {
					Key     string `msgpack:"key"`
					Payload []byte `msgpack:"payload"`
				}
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				puts++
				storedKey, storedPayload = request.Key, append([]byte(nil), request.Payload...)
				return msgpack.Marshal(map[string]any{"registry_revision": int64(1)})
			case "runtime-control/runtime-control.v1.runtime.get":
				return msgpack.Marshal(map[string]any{
					"version": "runtime-control.v1", "workload": workload, "generation": int64(7), "revision": int64(1),
				})
			case "runtime-control/runtime-control.v1.action.request":
				actionCalls++
				var request serverMutationRuntimeActionRequestV4
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				var envelope map[string]any
				if err := msgpack.Unmarshal(request.Payload, &envelope); err != nil {
					return nil, err
				}
				body, _ := envelope["body"].(map[string]any)
				lastOperation, _ = body["operation"].(string)
				return msgpack.Marshal(map[string]any{
					"version": "runtime-control.v1",
					"intent": map[string]any{
						"id": map[string]any{"version": "contracts.v1", "value": request.ID.Value},
					},
				})
			case "runtime-control/runtime-control.v1.effect.claim.exact":
				claimCalls++
				return msgpack.Marshal(map[string]any{
					"intent": map[string]any{
						"id": map[string]any{"version": "contracts.v1", "value": "intent-1"},
					},
					"fence": map[string]any{}, "lease_expires_at": "2026-07-26T12:02:00Z",
				})
			case "effects/server-mutation.host.execute.v4":
				hostCalls++
				return serverMutationHostResultV4(lastOperation)
			case "runtime-control/runtime-control.v1.effect.receipt.apply":
				applies++
				return msgpack.Marshal(map[string]any{
					"version": "runtime-control.v1", "status": "succeeded",
				})
			default:
				return nil, fmt.Errorf("unexpected owner call %s/%s", target, function)
			}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	payloadSum, projectionSum := sha256.Sum256(route), sha256.Sum256(projection)
	request := workflow.DispatchRequest{
		Event: "evolution.sessions.server.mutation.v4",
		Payload: map[string]any{
			"version":   "evolution.server-mutation-owner-adapter.v4",
			"operation": "minecraft.settings.apply",
			"payload":   route, "payload_sha256": hex.EncodeToString(payloadSum[:]),
			"projection": projection, "projection_sha256": hex.EncodeToString(projectionSum[:]),
		},
	}
	first, err := runtime.Dispatch(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.Dispatch(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Value, second.Value) {
		t.Fatalf("replay projection changed: %#v / %#v", first.Value, second.Value)
	}
	if actionCalls != 1 || claimCalls != 1 || hostCalls != 1 || puts != 1 || applies != 2 {
		t.Fatalf(
			"calls action=%d claim=%d host=%d put=%d apply=%d",
			actionCalls, claimCalls, hostCalls, puts, applies,
		)
	}
}

func TestServerMutationWorkflowsV4RuntimeRecreateUsesExactWorkloadClaimAndDurableReplay(t *testing.T) {
	authorization, _, workload, node, now, _ := serverMutationSettingsRouteValues()
	opaque := func(value string) map[string]any {
		return map[string]any{"version": "contracts.v1", "value": value}
	}
	route, err := msgpack.Marshal(map[string]any{
		"authorization": authorization, "command_id": opaque("recreate"),
		"idempotency": opaque("idem-recreate"), "workload": workload, "node": node,
		"now": now, "template": opaque("paper"), "resource_profile": opaque("standard"),
		"resources": map[string]any{},
		"minecraft": map[string]any{
			"version": "evolution.minecraft.server-mutation.v4", "kind": "recreate",
			"body": map[string]any{
				"operation": "minecraft.recreate", "payload": map[string]any{"requested": "recreate"},
				"projection": map[string]any{"status": "recreating"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := serverMutationProjectionWireV4(
		t, 200, map[string]any{"status": "recreating"},
	)
	var storedKey string
	var storedPayload []byte
	var reconfigures, claims, hosts, puts, applies int
	runtime, err := New(Options{
		Script: serverMutationWorkflowsV4Lua(t),
		Caller: CallFunc(func(target, function string, wire []byte) ([]byte, error) {
			switch target + "/" + function {
			case "configuration-registry/configuration-registry.v1.fact.get":
				var request struct {
					Key string `msgpack:"key"`
				}
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				if storedKey == request.Key && len(storedPayload) != 0 {
					return msgpack.Marshal(map[string]any{
						"found": true,
						"fact": map[string]any{
							"family": "config", "key": storedKey,
							"payload": storedPayload, "revision": int64(1),
						},
					})
				}
				return msgpack.Marshal(map[string]any{"found": false})
			case "configuration-registry/configuration-registry.v1.fact.put":
				var request struct {
					Key     string `msgpack:"key"`
					Payload []byte `msgpack:"payload"`
				}
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				puts++
				storedKey, storedPayload = request.Key, append([]byte(nil), request.Payload...)
				return msgpack.Marshal(map[string]any{"registry_revision": int64(1)})
			case "workload-provisioning/workload-provisioning.v1.get":
				return msgpack.Marshal(map[string]any{
					"version": "workload-provisioning.v1", "workload": workload, "generation": int64(3),
				})
			case "workload-provisioning/workload-provisioning.v1.reconfigure":
				reconfigures++
				return msgpack.Marshal(map[string]any{
					"version": "workload-provisioning.v1",
					"effect": map[string]any{
						"intent": map[string]any{
							"id": map[string]any{"version": "contracts.v1", "value": "recreate-intent-1"},
						},
					},
				})
			case "workload-provisioning/workload-provisioning.v1.effect.claim.exact":
				claims++
				var request struct {
					Intent      map[string]any `msgpack:"intent"`
					LeaseMillis int64          `msgpack:"lease_millis"`
				}
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				if request.LeaseMillis != 120_000 ||
					request.Intent["value"] != "recreate-intent-1" {
					return nil, fmt.Errorf("exact workload claim was not intent-bound: %#v", request)
				}
				return msgpack.Marshal(map[string]any{
					"version": "workload-provisioning.v1", "kind": "effect.claim",
					"effect": map[string]any{
						"intent": map[string]any{
							"id": map[string]any{"version": "contracts.v1", "value": "recreate-intent-1"},
						},
					},
					"fence": map[string]any{}, "snapshot": nil,
				})
			case "effects/server-mutation.host.execute.v4":
				hosts++
				var request struct {
					Owner string `msgpack:"owner"`
				}
				if err := msgpack.Unmarshal(wire, &request); err != nil {
					return nil, err
				}
				if request.Owner != "workload-provisioning" {
					return nil, fmt.Errorf("host owner = %q", request.Owner)
				}
				return serverMutationHostResultV4("minecraft.recreate")
			case "workload-provisioning/workload-provisioning.v1.effect.receipt.apply":
				applies++
				return msgpack.Marshal(map[string]any{
					"version": "workload-provisioning.v1", "kind": "effect.receipt.apply",
				})
			default:
				return nil, fmt.Errorf("unexpected owner call %s/%s", target, function)
			}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	payloadSum, projectionSum := sha256.Sum256(route), sha256.Sum256(projection)
	request := workflow.DispatchRequest{
		Event: "evolution.sessions.server.mutation.v4",
		Payload: map[string]any{
			"version": "evolution.server-mutation-owner-adapter.v4", "operation": "minecraft.recreate",
			"payload": route, "payload_sha256": hex.EncodeToString(payloadSum[:]),
			"projection": projection, "projection_sha256": hex.EncodeToString(projectionSum[:]),
		},
	}
	first, err := runtime.Dispatch(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.Dispatch(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Value, second.Value) {
		t.Fatalf("recreate replay projection changed: %#v / %#v", first.Value, second.Value)
	}
	if reconfigures != 2 || claims != 1 || hosts != 1 || puts != 1 || applies != 2 {
		t.Fatalf(
			"calls reconfigure=%d claim=%d host=%d put=%d apply=%d",
			reconfigures, claims, hosts, puts, applies,
		)
	}
}

func TestServerMutationWorkflowsV4RuntimeWhitelistResolvesOnceAndProjectsDurableReceipt(t *testing.T) {
	for _, test := range []struct {
		platform, query, canonical, uuid, added string
	}{
		{"java", "notch", "Notch", "8667ba71-b85a-4004-af54-457a9734eed7", "Notch"},
		{"bedrock", "Bedrock User", "Bedrock User", "00000000-0000-0000-0009-01f123456789", ".Bedrock User"},
	} {
		t.Run(test.platform, func(t *testing.T) {
			authorization, _, workload, node, now, _ := serverMutationSettingsRouteValues()
			opaque := func(value string) map[string]any {
				return map[string]any{"version": "contracts.v1", "value": value}
			}
			route, err := msgpack.Marshal(map[string]any{
				"authorization": authorization, "command_id": opaque("whitelist-" + test.platform),
				"idempotency": opaque("idem-whitelist-" + test.platform),
				"workload":    workload, "node": node, "now": now,
				"minecraft": map[string]any{
					"version": "evolution.minecraft.server-mutation.v4", "kind": "access",
					"body": map[string]any{
						"operation": "minecraft.access.apply",
						"payload": map[string]any{
							"action": "whitelist.add", "player_name": test.query, "platform": test.platform,
						},
						"projection": map[string]any{},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			projection := serverMutationProjectionWireV4(t, 200, map[string]any{})
			var storedKey string
			var storedPayload []byte
			var resolves, actions, claims, hosts, puts, applies int
			runtime, err := New(Options{
				Script: serverMutationWorkflowsV4Lua(t),
				Caller: CallFunc(func(target, function string, wire []byte) ([]byte, error) {
					switch target + "/" + function {
					case "configuration-registry/configuration-registry.v1.fact.get":
						var request struct {
							Key string `msgpack:"key"`
						}
						if err := msgpack.Unmarshal(wire, &request); err != nil {
							return nil, err
						}
						if storedKey == request.Key && len(storedPayload) != 0 {
							return msgpack.Marshal(map[string]any{
								"found": true,
								"fact": map[string]any{
									"family": "config", "key": storedKey,
									"payload": storedPayload, "revision": int64(1),
								},
							})
						}
						return msgpack.Marshal(map[string]any{"found": false})
					case "configuration-registry/configuration-registry.v1.fact.put":
						var request struct {
							Key     string `msgpack:"key"`
							Payload []byte `msgpack:"payload"`
						}
						if err := msgpack.Unmarshal(wire, &request); err != nil {
							return nil, err
						}
						puts++
						storedKey, storedPayload = request.Key, append([]byte(nil), request.Payload...)
						return msgpack.Marshal(map[string]any{"registry_revision": int64(1)})
					case "player-identity-resolver/player-identity.v1.resolve":
						resolves++
						var request struct {
							Version    string `msgpack:"version"`
							PlayerName string `msgpack:"player_name"`
							Platform   string `msgpack:"platform"`
						}
						if err := msgpack.Unmarshal(wire, &request); err != nil {
							return nil, err
						}
						if request.Version != "player-identity.v1" ||
							request.PlayerName != test.query || request.Platform != test.platform {
							return nil, fmt.Errorf("identity request = %#v", request)
						}
						return msgpack.Marshal(map[string]any{
							"version": "player-identity.v1", "uuid": test.uuid,
							"name": test.canonical, "platform": test.platform,
						})
					case "runtime-control/runtime-control.v1.runtime.get":
						return msgpack.Marshal(map[string]any{
							"version": "runtime-control.v1", "workload": workload, "generation": int64(7), "revision": int64(1),
						})
					case "runtime-control/runtime-control.v1.action.request":
						actions++
						var request serverMutationRuntimeActionRequestV4
						if err := msgpack.Unmarshal(wire, &request); err != nil {
							return nil, err
						}
						var envelope map[string]any
						if err := msgpack.Unmarshal(request.Payload, &envelope); err != nil {
							return nil, err
						}
						body, _ := envelope["body"].(map[string]any)
						payload, _ := body["payload"].(map[string]any)
						if payload["action"] != "whitelist.add" ||
							payload["name"] != test.canonical ||
							payload["uuid"] != test.uuid ||
							payload["platform"] != test.platform {
							return nil, fmt.Errorf("canonical whitelist payload = %#v", payload)
						}
						return msgpack.Marshal(map[string]any{
							"version": "runtime-control.v1",
							"intent": map[string]any{
								"id": map[string]any{"version": "contracts.v1", "value": request.ID.Value},
							},
						})
					case "runtime-control/runtime-control.v1.effect.claim.exact":
						claims++
						return msgpack.Marshal(map[string]any{
							"intent": map[string]any{
								"id": map[string]any{"version": "contracts.v1", "value": "intent-1"},
							},
							"fence": map[string]any{}, "lease_expires_at": "2026-07-26T12:02:00Z",
						})
					case "effects/server-mutation.host.execute.v4":
						hosts++
						return serverMutationWhitelistHostResultV4(test.added, test.uuid)
					case "runtime-control/runtime-control.v1.effect.receipt.apply":
						applies++
						return msgpack.Marshal(map[string]any{
							"version": "runtime-control.v1", "status": "succeeded",
						})
					default:
						return nil, fmt.Errorf("unexpected owner call %s/%s", target, function)
					}
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			payloadSum, projectionSum := sha256.Sum256(route), sha256.Sum256(projection)
			request := workflow.DispatchRequest{
				Event: "evolution.sessions.server.mutation.v4",
				Payload: map[string]any{
					"version":   "evolution.server-mutation-owner-adapter.v4",
					"operation": "minecraft.access.apply",
					"payload":   route, "payload_sha256": hex.EncodeToString(payloadSum[:]),
					"projection": projection, "projection_sha256": hex.EncodeToString(projectionSum[:]),
				},
			}
			first, err := runtime.Dispatch(request)
			if err != nil {
				t.Fatal(err)
			}
			second, err := runtime.Dispatch(request)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]any{
				"status":  int64(200),
				"headers": map[string]any{"Content-Type": "application/json"},
				"body":    map[string]any{"added": test.added, "uuid": test.uuid},
			}
			if !reflect.DeepEqual(first.Value, want) || !reflect.DeepEqual(first.Value, second.Value) {
				t.Fatalf("whitelist projection/replay = %#v / %#v", first.Value, second.Value)
			}
			if resolves != 1 || actions != 1 || claims != 1 ||
				hosts != 1 || puts != 1 || applies != 2 {
				t.Fatalf(
					"calls resolver=%d action=%d claim=%d host=%d put=%d apply=%d",
					resolves, actions, claims, hosts, puts, applies,
				)
			}
		})
	}
}

func TestServerMutationWorkflowsV4RuntimeWhitelistRejectsPlatformIdentityMismatch(t *testing.T) {
	authorization, _, workload, node, now, _ := serverMutationSettingsRouteValues()
	opaque := func(value string) map[string]any {
		return map[string]any{"version": "contracts.v1", "value": value}
	}
	route, err := msgpack.Marshal(map[string]any{
		"authorization": authorization, "command_id": opaque("whitelist-mismatch"),
		"idempotency": opaque("idem-whitelist-mismatch"),
		"workload":    workload, "node": node, "now": now,
		"minecraft": map[string]any{
			"version": "evolution.minecraft.server-mutation.v4", "kind": "access",
			"body": map[string]any{
				"operation": "minecraft.access.apply",
				"payload": map[string]any{
					"action": "whitelist.add", "player_name": "Bedrock User", "platform": "bedrock",
				},
				"projection": map[string]any{},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	projection := serverMutationProjectionWireV4(t, 200, map[string]any{})
	var hostCalls int
	runtime, err := New(Options{
		Script: serverMutationWorkflowsV4Lua(t),
		Caller: CallFunc(func(target, function string, _ []byte) ([]byte, error) {
			switch target + "/" + function {
			case "configuration-registry/configuration-registry.v1.fact.get":
				return msgpack.Marshal(map[string]any{"found": false})
			case "player-identity-resolver/player-identity.v1.resolve":
				return msgpack.Marshal(map[string]any{
					"version": "player-identity.v1",
					"uuid":    "8667ba71-b85a-4004-af54-457a9734eed7",
					"name":    "Bedrock User", "platform": "bedrock",
				})
			case "effects/server-mutation.host.execute.v4":
				hostCalls++
				return nil, fmt.Errorf("host must not execute")
			default:
				return nil, fmt.Errorf("unexpected owner call %s/%s", target, function)
			}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	payloadSum, projectionSum := sha256.Sum256(route), sha256.Sum256(projection)
	_, err = runtime.Dispatch(workflow.DispatchRequest{
		Event: "evolution.sessions.server.mutation.v4",
		Payload: map[string]any{
			"version":   "evolution.server-mutation-owner-adapter.v4",
			"operation": "minecraft.access.apply",
			"payload":   route, "payload_sha256": hex.EncodeToString(payloadSum[:]),
			"projection": projection, "projection_sha256": hex.EncodeToString(projectionSum[:]),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "player identity does not match platform") ||
		hostCalls != 0 {
		t.Fatalf("platform mismatch error/host = %v/%d", err, hostCalls)
	}
}
