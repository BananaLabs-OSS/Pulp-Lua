package orchestrator

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func TestRuntimeStateCommandsAndEvents(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("increment", function(payload)
  local count = (pulp.state_get("count") or 0) + payload.amount
  pulp.state_set("count", count)
  pulp.command("counter.persist", { count = count })
  pulp.emit("counter.changed", { count = count })
  return { count = count }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	for attempt, want := range []int64{2, 4} {
		result, err := runtime.Dispatch(DispatchRequest{
			Event:   "increment",
			Payload: map[string]any{"amount": int64(2)},
		})
		if err != nil {
			t.Fatalf("Dispatch %d: %v", attempt, err)
		}
		value := result.Value.(map[string]any)
		if value["count"] != want {
			t.Fatalf("count = %#v, want %d", value["count"], want)
		}
		if len(result.Commands) != 1 || result.Commands[0].Name != "counter.persist" {
			t.Fatalf("commands = %#v", result.Commands)
		}
		if len(result.Events) != 1 || result.Events[0].Name != "counter.changed" {
			t.Fatalf("events = %#v", result.Events)
		}
	}
}

func TestRuntimeCallsSiblingWithMessagePack(t *testing.T) {
	var calledTarget, calledFunction string
	caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		calledTarget, calledFunction = target, function
		var request map[string]any
		if err := msgpack.Unmarshal(payload, &request); err != nil {
			return nil, err
		}
		return msgpack.Marshal(map[string]any{"value": request["value"].(int64) * 2})
	})
	runtime, err := New(Options{
		Caller: caller,
		Script: `
pulp.on("double", function(payload)
  return pulp.call("math-engine", "math.double", payload)
end)
`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{
		Event:   "double",
		Payload: map[string]any{"value": int64(21)},
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calledTarget != "math-engine" || calledFunction != "math.double" {
		t.Fatalf("call = %s/%s", calledTarget, calledFunction)
	}
	if result.Value.(map[string]any)["value"] != int64(42) {
		t.Fatalf("result = %#v", result.Value)
	}
}

func TestRuntimeRejectsUnsafeGenericNumbersBeforeLua(t *testing.T) {
	runtime, err := New(Options{
		Script: `pulp.on("echo", function(payload) return payload end)`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	for name, value := range map[string]any{
		"inexact integer": maxExactLuaInteger + 1,
		"NaN":             math.NaN(),
		"positive Inf":    math.Inf(1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := runtime.Dispatch(DispatchRequest{
				Event:   "echo",
				Payload: map[string]any{"value": value},
			})
			if err == nil || !strings.Contains(err.Error(), "lower payload") {
				t.Fatalf("Dispatch error = %v", err)
			}
		})
	}
}

func TestRuntimeRejectsUnsafeGenericSiblingResponse(t *testing.T) {
	caller := CallFunc(func(_, _ string, _ []byte) ([]byte, error) {
		return msgpack.Marshal(map[string]any{
			"counter": uint64(maxExactLuaUnsigned + 1),
		})
	})
	runtime, err := New(Options{
		Caller: caller,
		Script: `
pulp.on("unsafe-response", function()
  return pulp.call("counter-engine", "counter.read", {})
end)
`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	_, err = runtime.Dispatch(DispatchRequest{Event: "unsafe-response"})
	if err == nil ||
		!strings.Contains(err.Error(), "pulp.call: lower response") ||
		!strings.Contains(err.Error(), "outside Lua's exact integer range") {
		t.Fatalf("Dispatch error = %v", err)
	}
}

func TestRuntimeCallRawPreservesOpaqueBytes(t *testing.T) {
	requestWire := []byte{0x00, 0xff, 0xc1, 'r', 'a', 'w'}
	responseWire := []byte{0x92, 0x00, 0xff, 0xc1}
	caller := CallFunc(func(target, function string, payload []byte) ([]byte, error) {
		if target != "typed-engine" || function != "typed.call" {
			t.Fatalf("call = %s/%s", target, function)
		}
		if !bytes.Equal(payload, requestWire) {
			t.Fatalf("payload = %v, want %v", payload, requestWire)
		}
		return responseWire, nil
	})
	runtime, err := New(Options{
		Caller: caller,
		Script: `
pulp.on("raw", function(payload)
  return pulp.call_raw("typed-engine", "typed.call", payload.request_msgpack)
end)
`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{
		Event: "raw",
		Payload: map[string]any{
			"request_msgpack": requestWire,
		},
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	got, ok := result.Value.(string)
	if !ok || !bytes.Equal([]byte(got), responseWire) {
		t.Fatalf("raw result = %T %v, want %v", result.Value, result.Value, responseWire)
	}
}

func TestRuntimeAppCallRawPreservesOpaqueBytesAndExplicitAddress(t *testing.T) {
	requestWire := []byte{0x00, 0xff, 0xc1, 'r', 'a', 'w'}
	responseWire := []byte{0x92, 0x00, 0xff, 0xc1}
	var calls int
	caller := AppCallFunc(func(app, instance, cell, provider string, payload []byte) ([]byte, error) {
		calls++
		if app != "sessions" || instance != "blue" || cell != "commerce" || provider != "order.read.v1" {
			t.Fatalf("address = %s/%s/%s/%s", app, instance, cell, provider)
		}
		if !bytes.Equal(payload, requestWire) {
			t.Fatalf("payload = %v, want %v", payload, requestWire)
		}
		return responseWire, nil
	})
	runtime, err := New(Options{
		AppCaller: caller,
		Script: `
pulp.on("raw", function(payload)
  return pulp.app_call_raw("sessions", "blue", "commerce", "order.read.v1", payload.request_msgpack)
end)
`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{
		Event:   "raw",
		Payload: map[string]any{"request_msgpack": requestWire},
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	got, ok := result.Value.(string)
	if !ok || !bytes.Equal([]byte(got), responseWire) {
		t.Fatalf("raw result = %T %v, want %v", result.Value, result.Value, responseWire)
	}
}

func TestRuntimeAppCallRawFailsClosedWithoutHostImport(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("cross", function()
  return pulp.app_call_raw("sessions", "blue", "commerce", "order.read.v1", "")
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	_, err = runtime.Dispatch(DispatchRequest{Event: "cross"})
	if err == nil || !strings.Contains(err.Error(), "no cross-application caller is configured") {
		t.Fatalf("Dispatch error = %v", err)
	}
}

func TestRuntimeAppCallRawRejectsMalformedAmbientAddress(t *testing.T) {
	calls := 0
	runtime, err := New(Options{
		AppCaller: AppCallFunc(func(_, _, _, _ string, _ []byte) ([]byte, error) {
			calls++
			return nil, nil
		}),
		Script: `
assert(pulp.apps == nil)
assert(pulp.app_list == nil)
pulp.on("missing-instance", function()
  return pulp.app_call_raw("sessions", "", "commerce", "order.read.v1", "")
end)
pulp.on("bad-payload", function()
  return pulp.app_call_raw("sessions", "blue", "commerce", "order.read.v1", {})
end)
pulp.on("too-many", function()
  return pulp.app_call_raw("sessions", "blue", "commerce", "order.read.v1", "", "extra")
end)
`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	for _, event := range []string{"missing-instance", "bad-payload", "too-many"} {
		if _, err := runtime.Dispatch(DispatchRequest{Event: event}); err == nil || !strings.Contains(err.Error(), "pulp.app_call_raw") {
			t.Fatalf("%s error = %v", event, err)
		}
	}
	if calls != 0 {
		t.Fatalf("host caller invoked %d times for rejected input", calls)
	}
}

func TestRuntimeAppCallRawBoundsPayloadAndResponse(t *testing.T) {
	tooLarge := strings.Repeat("x", maxAppCallPayload+1)
	runtime, err := New(Options{
		AppCaller: AppCallFunc(func(_, _, _, _ string, _ []byte) ([]byte, error) {
			return []byte(tooLarge), nil
		}),
		Script: `
pulp.on("large-request", function(payload)
  return pulp.app_call_raw("sessions", "blue", "commerce", "order.read.v1", payload.raw)
end)
pulp.on("large-response", function()
  return pulp.app_call_raw("sessions", "blue", "commerce", "order.read.v1", "")
end)
`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	if _, err := runtime.Dispatch(DispatchRequest{Event: "large-request", Payload: map[string]any{"raw": tooLarge}}); err == nil || !strings.Contains(err.Error(), "payload exceeds") {
		t.Fatalf("large request error = %v", err)
	}
	if _, err := runtime.Dispatch(DispatchRequest{Event: "large-response"}); err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("large response error = %v", err)
	}
}

func TestRuntimeSandboxHidesAmbientAuthority(t *testing.T) {
	runtime, err := New(Options{Script: `
assert(os == nil)
assert(io == nil)
assert(package == nil)
assert(debug == nil)
assert(dofile == nil)
assert(loadfile == nil)
assert(loadstring == nil)
assert(math.random == nil)
pulp.on("safe", function() return { ok = true } end)
`})
	if err != nil {
		t.Fatalf("sandbox script: %v", err)
	}
	runtime.Close()
}

func TestRuntimeTimeoutStopsRunawayHandler(t *testing.T) {
	runtime, err := New(Options{
		Timeout: 20 * time.Millisecond,
		Script:  `pulp.on("loop", function() while true do end end)`,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	_, err = runtime.Dispatch(DispatchRequest{Event: "loop"})
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("runaway error = %v", err)
	}
}
