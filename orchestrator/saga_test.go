package orchestrator

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRuntimePackUnpackPreservesMessagePackBytes(t *testing.T) {
	wire, err := msgpack.Marshal(map[string]any{"count": int64(41)})
	if err != nil {
		t.Fatalf("marshal wire: %v", err)
	}
	runtime, err := New(Options{Script: `
pulp.on("codec", function(payload)
  local decoded = pulp.unpack(payload.wire)
  return pulp.pack({ count = decoded.count + 1 })
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{Event: "codec", Payload: map[string]any{"wire": wire}})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	packed, ok := result.Value.(string)
	if !ok {
		t.Fatalf("packed value = %T", result.Value)
	}
	var decoded map[string]any
	if err := msgpack.Unmarshal([]byte(packed), &decoded); err != nil {
		t.Fatalf("decode packed response: %v", err)
	}
	if decoded["count"] != int64(42) {
		t.Fatalf("decoded response = %#v", decoded)
	}

}

func TestRuntimeRawEmbedsOnlyValidatedPrivateMessagePack(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("embed", function()
  local payload = pulp.pack({ amount_cents = 1200, currency = "usd" })
  return pulp.pack({ version = "pulp.effect.v1", payload = pulp.raw(payload) })
end)
pulp.on("escape", function()
  return pulp.raw(pulp.pack({ value = "private" }))
end)
pulp.on("foreign", function()
  return pulp.pack(foreign_userdata)
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{Event: "embed"})
	if err != nil {
		t.Fatalf("embed Dispatch: %v", err)
	}
	type envelope struct {
		Version string             `msgpack:"version"`
		Payload msgpack.RawMessage `msgpack:"payload"`
	}
	var decoded envelope
	if err := msgpack.Unmarshal([]byte(result.Value.(string)), &decoded); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	var payload map[string]any
	if err := msgpack.Unmarshal(decoded.Payload, &payload); err != nil {
		t.Fatalf("decode embedded payload: %v", err)
	}
	if decoded.Version != "pulp.effect.v1" || payload["amount_cents"] != int64(1200) || payload["currency"] != "usd" {
		t.Fatalf("decoded envelope = %#v payload=%#v", decoded, payload)
	}

	if _, err := runtime.Dispatch(DispatchRequest{Event: "escape"}); err == nil || !strings.Contains(err.Error(), "unsupported Lua value type userdata") {
		t.Fatalf("private raw userdata escaped handler: %v", err)
	}
	foreign := runtime.lua.NewUserData()
	foreign.Value = struct{}{}
	runtime.lua.SetGlobal("foreign_userdata", foreign)
	if _, err := runtime.Dispatch(DispatchRequest{Event: "foreign"}); err == nil || !strings.Contains(err.Error(), "unsupported Lua userdata") {
		t.Fatalf("foreign userdata entered pulp.pack: %v", err)
	}
}

func TestRuntimeExecuteSagaSequencesTypedCalls(t *testing.T) {
	calls := make([]string, 0, 2)
	caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		calls = append(calls, target+"/"+function)
		var request map[string]any
		if err := msgpack.Unmarshal(payload, &request); err != nil {
			return nil, err
		}
		switch target {
		case "commerce":
			if request["order_id"] != "order-1" {
				t.Fatalf("commerce request = %#v", request)
			}
			return msgpack.Marshal(map[string]any{"checkout_id": "checkout-1"})
		case "stripe":
			if request["checkout_id"] != "checkout-1" {
				t.Fatalf("stripe request = %#v", request)
			}
			return msgpack.Marshal(map[string]any{"payment_intent_id": "pi_123", "client_secret": "pi_secret_123"})
		default:
			t.Fatalf("unexpected target %q", target)
			return nil, nil
		}
	})
	runtime, err := New(Options{Caller: caller, Script: `
pulp.on("sessions.checkout.begin.v1", function(payload)
  local checkout = pulp.unpack(pulp.call_raw("commerce", "checkout.begin", pulp.pack({ order_id = payload.order_id })))
  local payment = pulp.unpack(pulp.call_raw("stripe", "payment_intent.create", pulp.pack({ checkout_id = checkout.checkout_id })))
  return {
    status = "completed",
    result = pulp.pack({ checkout_id = checkout.checkout_id, client_secret = payment.client_secret }),
    effects = {{
      id = "effect-1",
      kind = "stripe.payment_intent.create",
      idempotency_key = "checkout:order-1",
      payload = pulp.pack({ checkout_id = checkout.checkout_id }),
      acknowledgement = {
        status = "completed",
        result = pulp.pack(payment),
      },
    }},
  }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	request, err := workflow.NewSagaRequest("sessions.checkout.begin.v1", "request-1", "checkout:order-1", map[string]string{"order_id": "order-1"})
	if err != nil {
		t.Fatalf("NewSagaRequest: %v", err)
	}
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("ExecuteSaga: %v", err)
	}
	if got, want := strings.Join(calls, ","), "commerce/checkout.begin,stripe/payment_intent.create"; got != want {
		t.Fatalf("call sequence = %q, want %q", got, want)
	}
	type response struct {
		CheckoutID   string `msgpack:"checkout_id"`
		ClientSecret string `msgpack:"client_secret"`
	}
	decoded, err := workflow.DecodeResult[response](result)
	if err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if decoded.CheckoutID != "checkout-1" || decoded.ClientSecret != "pi_secret_123" {
		t.Fatalf("result = %#v", decoded)
	}
	if len(result.Effects) != 1 || result.Effects[0].Acknowledgement.Status != workflow.EffectCompleted {
		t.Fatalf("effects = %#v", result.Effects)
	}
}

func TestRuntimeExecuteSagaStatesAndIdempotency(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("pending", function() return { status = "pending", effects = {{ id = "effect-1", kind = "email.send", idempotency_key = "email:1", payload = pulp.pack({ template = "ready" }), acknowledgement = { status = "pending" } }} } end)
pulp.on("failed", function() return { status = "failed", error = { code = "unavailable", message = "try again" } } end)
pulp.on("once", function()
  local count = (pulp.state_get("count") or 0) + 1
  pulp.state_set("count", count)
  return { status = "completed", result = pulp.pack({ count = count }) }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	for _, want := range []struct {
		name   string
		status workflow.SagaStatus
	}{
		{name: "pending", status: workflow.SagaPending},
		{name: "failed", status: workflow.SagaFailed},
	} {
		request, err := workflow.NewSagaRequest(want.name, want.name+"-request", want.name+":key", map[string]any{})
		if err != nil {
			t.Fatalf("NewSagaRequest(%s): %v", want.name, err)
		}
		result, err := runtime.ExecuteSaga(request)
		if err != nil {
			t.Fatalf("ExecuteSaga(%s): %v", want.name, err)
		}
		if result.Status != want.status {
			t.Fatalf("%s status = %q", want.name, result.Status)
		}
	}

	request, err := workflow.NewSagaRequest("once", "request-1", "once:key", map[string]any{})
	if err != nil {
		t.Fatalf("NewSagaRequest: %v", err)
	}
	first, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("first ExecuteSaga: %v", err)
	}
	second, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("second ExecuteSaga: %v", err)
	}
	if !bytes.Equal(first.Result, second.Result) {
		t.Fatalf("idempotent result changed: %v != %v", first.Result, second.Result)
	}
	type countResult struct {
		Count int64 `msgpack:"count"`
	}
	decoded, err := workflow.DecodeResult[countResult](second)
	if err != nil || decoded.Count != 1 {
		t.Fatalf("idempotent count = %#v, %v", decoded, err)
	}

	differentRequest, err := workflow.NewSagaRequest("once", "request-2", "once:key", map[string]any{})
	if err != nil {
		t.Fatalf("NewSagaRequest mismatch: %v", err)
	}
	if _, err := runtime.ExecuteSaga(differentRequest); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("idempotency conflict error = %v", err)
	}
}

func TestRuntimeExecuteSagaReentersPendingAndCachesTerminalOutcome(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("settle", function()
  local count = (pulp.state_get("settle-count") or 0) + 1
  pulp.state_set("settle-count", count)
  if count == 1 then
    return { status = "pending", effects = {{ id = "effect-1", kind = "email.send", idempotency_key = "email:1", payload = pulp.pack({ template = "ready" }), acknowledgement = { status = "pending" } }} }
  end
  return { status = "completed", result = pulp.pack({ count = count }) }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	request, err := workflow.NewSagaRequest("settle", "settle-request", "settle:key", map[string]any{})
	if err != nil {
		t.Fatalf("NewSagaRequest: %v", err)
	}
	first, err := runtime.ExecuteSaga(request)
	if err != nil || first.Status != workflow.SagaPending {
		t.Fatalf("first ExecuteSaga = %#v, %v", first, err)
	}
	second, err := runtime.ExecuteSaga(request)
	if err != nil || second.Status != workflow.SagaCompleted {
		t.Fatalf("second ExecuteSaga = %#v, %v", second, err)
	}
	third, err := runtime.ExecuteSaga(request)
	if err != nil || !bytes.Equal(second.Result, third.Result) {
		t.Fatalf("terminal replay = %#v, %v", third, err)
	}
	type countResult struct {
		Count int64 `msgpack:"count"`
	}
	decoded, err := workflow.DecodeResult[countResult](third)
	if err != nil || decoded.Count != 2 {
		t.Fatalf("terminal count = %#v, %v", decoded, err)
	}
}

func TestRuntimeCurrentSagaIsExactAndScopedToSagaExecution(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("forward", function()
  local saga = pulp.current_saga()
  return { status = "completed", result = pulp.pack(saga) }
end)
pulp.on("event", function()
  return pulp.current_saga()
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	request, err := workflow.NewSagaRequest("forward", "request-original", "checkout:original", map[string]any{"value": "one"})
	if err != nil {
		t.Fatalf("NewSagaRequest: %v", err)
	}
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("ExecuteSaga: %v", err)
	}
	var context map[string]any
	if err := msgpack.Unmarshal(result.Result, &context); err != nil {
		t.Fatalf("decode saga context: %v", err)
	}
	if context["request_id"] != request.RequestID || context["idempotency_key"] != request.IdempotencyKey || context["workflow"] != request.Version || context["event"] != request.Name {
		t.Fatalf("saga context = %#v", context)
	}
	if _, err := runtime.Dispatch(DispatchRequest{Event: "event"}); err == nil || !strings.Contains(err.Error(), "only valid while executing a saga") {
		t.Fatalf("current_saga leaked outside ExecuteSaga: %v", err)
	}
}

func TestRuntimeAppExecuteSagaForwardsOriginalIdentityAndValidatesResult(t *testing.T) {
	var calledApp, calledInstance, calledCell, calledProvider string
	caller := AppCallFunc(func(app, instance, cell, provider string, payload []byte) ([]byte, error) {
		calledApp, calledInstance, calledCell, calledProvider = app, instance, cell, provider
		var forwarded workflow.SagaRequest
		if err := msgpack.Unmarshal(payload, &forwarded); err != nil {
			return nil, err
		}
		result, err := workflow.NewCompletedSagaResult(forwarded, map[string]any{"owner": "sessions"}, nil)
		if err != nil {
			return nil, err
		}
		return msgpack.Marshal(result)
	})
	runtime, err := New(Options{AppCaller: caller, Script: `
pulp.on("checkout", function()
  local before = pulp.current_saga()
  local remote = pulp.unpack(pulp.app_execute_saga("sessions", "primary", "lua"))
  local after = pulp.current_saga()
  return {
    status = "completed",
    result = pulp.pack({
      request_id = remote.request_id,
      idempotency_key = remote.idempotency_key,
      before = before.request_id,
      after = after.request_id,
      remote_status = remote.status,
    }),
  }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	request, err := workflow.NewSagaRequest("checkout", "request-original", "checkout:original", map[string]any{"value": "one"})
	if err != nil {
		t.Fatalf("NewSagaRequest: %v", err)
	}
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatalf("ExecuteSaga: %v", err)
	}
	if calledApp != "sessions" || calledInstance != "primary" || calledCell != "lua" || calledProvider != workflow.FnExecuteSaga {
		t.Fatalf("cross-app call = %s/%s/%s/%s", calledApp, calledInstance, calledCell, calledProvider)
	}
	var value map[string]any
	if err := msgpack.Unmarshal(result.Result, &value); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if value["request_id"] != request.RequestID || value["idempotency_key"] != request.IdempotencyKey || value["before"] != request.RequestID || value["after"] != request.RequestID || value["remote_status"] != string(workflow.SagaCompleted) {
		t.Fatalf("result = %#v", value)
	}
}

func TestRuntimeAppExecuteSagaCanForwardCanonicalResultWithoutRawMessageLoss(t *testing.T) {
	responseWire, err := msgpack.Marshal(map[string]any{"status": int64(200), "body": []byte(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	caller := AppCallFunc(func(_, _, _, _ string, payload []byte) ([]byte, error) {
		var request workflow.SagaRequest
		if err := msgpack.Unmarshal(payload, &request); err != nil {
			return nil, err
		}
		result, err := workflow.NewCompletedSagaResult(request, responseWire, nil)
		if err != nil {
			return nil, err
		}
		return msgpack.Marshal(result)
	})
	runtime, err := New(Options{AppCaller: caller, Script: `
pulp.on("checkout", function()
  return pulp.app_execute_saga("sessions", "primary", "lua")
end)
`})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	request, err := workflow.NewSagaRequest("checkout", "request-1", "attempt-1", map[string]any{"request_msgpack": "opaque"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ExecuteSaga(request)
	if err != nil {
		t.Fatal(err)
	}
	got, err := workflow.DecodeResult[[]byte](result)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(responseWire) {
		t.Fatalf("forwarded result bytes changed: got %x want %x", got, responseWire)
	}
}

func TestRuntimeCurrentSagaClearsAfterHandlerErrorAndRejectsMismatchedRemoteIdentity(t *testing.T) {
	caller := AppCallFunc(func(_, _, _, _ string, payload []byte) ([]byte, error) {
		var request workflow.SagaRequest
		if err := msgpack.Unmarshal(payload, &request); err != nil {
			return nil, err
		}
		request.RequestID = "invented-id"
		result, err := workflow.NewCompletedSagaResult(request, map[string]any{"ok": true}, nil)
		if err != nil {
			return nil, err
		}
		return msgpack.Marshal(result)
	})
	runtime, err := New(Options{AppCaller: caller, Script: `
pulp.on("broken", function() local _ = pulp.current_saga(); error("expected failure") end)
pulp.on("mismatch", function() return pulp.app_execute_saga("sessions", "primary", "lua") end)
pulp.on("event", function() return pulp.current_saga() end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	for _, name := range []string{"broken", "mismatch"} {
		request, err := workflow.NewSagaRequest(name, name+"-request", name+":key", map[string]any{"value": true})
		if err != nil {
			t.Fatalf("NewSagaRequest(%s): %v", name, err)
		}
		if _, err := runtime.ExecuteSaga(request); err == nil {
			t.Fatalf("ExecuteSaga(%s) unexpectedly succeeded", name)
		}
	}
	if _, err := runtime.Dispatch(DispatchRequest{Event: "event"}); err == nil || !strings.Contains(err.Error(), "only valid while executing a saga") {
		t.Fatalf("current_saga leaked after error: %v", err)
	}
}

func TestRuntimeCurrentSagaDoesNotLeakAcrossConcurrentExecuteSagaCalls(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("concurrent", function()
  local saga = pulp.current_saga()
  return { status = "completed", result = pulp.pack({ request_id = saga.request_id, key = saga.idempotency_key }) }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	const count = 16
	errs := make(chan error, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			request, err := workflow.NewSagaRequest("concurrent", fmt.Sprintf("request-%d", i), fmt.Sprintf("key-%d", i), map[string]any{"index": i})
			if err != nil {
				errs <- err
				return
			}
			result, err := runtime.ExecuteSaga(request)
			if err != nil {
				errs <- err
				return
			}
			var value map[string]any
			if err := msgpack.Unmarshal(result.Result, &value); err != nil {
				errs <- err
				return
			}
			if value["request_id"] != request.RequestID || value["key"] != request.IdempotencyKey {
				errs <- fmt.Errorf("request %d observed context %#v", i, value)
			}
		}(i)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
