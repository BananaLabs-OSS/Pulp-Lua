package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func evolutionForwardingLua(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "Evolution", "pulp-cell", "evolution-host.lua")
	script, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(script)
}

func TestEvolutionLuaForwardsExactPendingReviewPaymentApproval(t *testing.T) {
	source := evolutionForwardingLua(t)
	const event = "evolution.sessions.admin.pending-review.approve.final-http.v1"
	if !strings.Contains(source, `"`+event+`"`) {
		t.Fatalf("Evolution host is missing forwarding for %q", event)
	}
}

func TestEvolutionLuaForwardsDispatchAndSagaWithExactApplicationIdentity(t *testing.T) {
	var dispatchCalls, sagaCalls int
	caller := AppCallFunc(func(app, instance, cell, provider string, payload []byte) ([]byte, error) {
		if app != "sessions" || instance != "primary" || cell != "lua-orchestrator" {
			t.Fatalf("cross-app address = %s/%s/%s", app, instance, cell)
		}
		switch provider {
		case workflow.FnDispatch:
			dispatchCalls++
			var request workflow.DispatchRequest
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request.Event != "evolution.sessions.tiers.get.v1" {
				t.Fatalf("forwarded event = %q", request.Event)
			}
			return msgpack.Marshal(workflow.DispatchResult{Value: "sessions-response"})
		case workflow.FnExecuteSaga:
			sagaCalls++
			var request workflow.SagaRequest
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			result, err := workflow.NewCompletedSagaResult(request, []byte("checkout-response"), nil)
			if err != nil {
				t.Fatal(err)
			}
			return msgpack.Marshal(result)
		default:
			t.Fatalf("unexpected provider %q", provider)
			return nil, nil
		}
	})

	runtime, err := New(Options{AppCaller: caller, Script: evolutionForwardingLua(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	dispatched, err := runtime.Dispatch(workflow.DispatchRequest{
		Event:   "evolution.sessions.tiers.get.v1",
		Payload: map[string]any{"request_msgpack": "opaque"},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := workflow.DecodeValue[string](dispatched)
	if err != nil || value != "sessions-response" {
		t.Fatalf("dispatch value = %q, err = %v", value, err)
	}

	request, err := workflow.NewSagaRequest(
		"evolution.sessions.extend.checkout.v1",
		"request-1",
		"checkout-attempt-1",
		map[string]any{"request_msgpack": "opaque"},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := workflow.DecodeResult[[]byte](result)
	if err != nil || string(response) != "checkout-response" {
		t.Fatalf("saga response = %q, err = %v", response, err)
	}
	if dispatchCalls != 1 || sagaCalls != 1 {
		t.Fatalf("cross-app calls: dispatch=%d saga=%d", dispatchCalls, sagaCalls)
	}
}
