package orchestrator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestRuntimePackIsStableAcrossRunsAndInsertionOrders(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("canonical", function(payload)
  local nested = {}
  local value = {}
  if payload.reverse then
    nested.zulu = 3
    nested.alpha = 1
    value.zulu = "last"
    value.nested = nested
    value.alpha = "first"
  else
    nested.alpha = 1
    nested.zulu = 3
    value.alpha = "first"
    value.nested = nested
    value.zulu = "last"
  end
  return pulp.pack(value)
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	var canonical []byte
	for attempt := 0; attempt < 100; attempt++ {
		result, err := runtime.Dispatch(DispatchRequest{
			Event:   "canonical",
			Payload: map[string]any{"reverse": attempt%2 == 1},
		})
		if err != nil {
			t.Fatalf("Dispatch %d: %v", attempt, err)
		}
		packed := []byte(result.Value.(string))
		if attempt == 0 {
			canonical = append([]byte(nil), packed...)
			continue
		}
		if !bytes.Equal(packed, canonical) {
			t.Fatalf("attempt %d produced non-canonical bytes:\nfirst %x\n got  %x", attempt, canonical, packed)
		}
	}

	var decoded map[string]any
	if err := msgpack.Unmarshal(canonical, &decoded); err != nil {
		t.Fatalf("standard MessagePack decoder rejected canonical bytes: %v", err)
	}
	nested, ok := decoded["nested"].(map[string]any)
	if !ok || nested["alpha"] != int64(1) || nested["zulu"] != int64(3) {
		t.Fatalf("decoded nested map = %#v", decoded["nested"])
	}
}

func TestRuntimePackPreservesArraysIntegersAndRawMessagePack(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("types", function(payload)
  return pulp.pack({
    array = {3, 1, 2},
    count = 9007199254740991,
    embedded = pulp.raw(payload.typed_wire),
  })
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	typedWire, err := msgpack.Marshal(map[string]any{
		"binary": []byte{0x00, 0x7f, 0xff},
		"wide":   uint64(1<<63 + 7),
	})
	if err != nil {
		t.Fatalf("encode typed wire: %v", err)
	}
	result, err := runtime.Dispatch(DispatchRequest{
		Event:   "types",
		Payload: map[string]any{"typed_wire": typedWire},
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	type envelope struct {
		Array    []int64            `msgpack:"array"`
		Count    int64              `msgpack:"count"`
		Embedded msgpack.RawMessage `msgpack:"embedded"`
	}
	var decoded envelope
	if err := msgpack.Unmarshal([]byte(result.Value.(string)), &decoded); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if fmt.Sprint(decoded.Array) != "[3 1 2]" {
		t.Fatalf("array order changed: %v", decoded.Array)
	}
	if decoded.Count != maxExactLuaInteger {
		t.Fatalf("integer = %d, want %d", decoded.Count, maxExactLuaInteger)
	}
	var embedded struct {
		Binary []byte `msgpack:"binary"`
		Wide   uint64 `msgpack:"wide"`
	}
	if err := msgpack.Unmarshal(decoded.Embedded, &embedded); err != nil {
		t.Fatalf("decode embedded raw MessagePack: %v", err)
	}
	if !bytes.Equal(embedded.Binary, []byte{0x00, 0x7f, 0xff}) || embedded.Wide != uint64(1<<63+7) {
		t.Fatalf("embedded value = %#v", embedded)
	}
}

func TestRuntimePackEncodesBytesMarkerAsMessagePackBinary(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("bytes", function()
  return pulp.pack({ payload = pulp.bytes(pulp.pack({ value = "opaque" })) })
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{Event: "bytes"})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	var decoded struct {
		Payload []byte `msgpack:"payload"`
	}
	if err := msgpack.Unmarshal([]byte(result.Value.(string)), &decoded); err != nil {
		t.Fatalf("decode binary envelope: %v", err)
	}
	var payload map[string]any
	if err := msgpack.Unmarshal(decoded.Payload, &payload); err != nil {
		t.Fatalf("decode opaque bytes: %v", err)
	}
	if payload["value"] != "opaque" {
		t.Fatalf("decoded opaque bytes = %#v", payload)
	}
}

func TestRuntimePackFingerprintReplayForStatusServerPayload(t *testing.T) {
	runtime, err := New(Options{Script: `
local function status_payload(reverse)
  local opaque = {}
  local workload = {}
  local server = {}
  local signal = {}
  local result = {}

  if reverse then
    opaque.region = "us-central"
    opaque.assignment = "match-17"
    workload.version = "contracts.v1"
    workload.id = "server-42"
    server.opaque = opaque
    server.generation = 8
    server.workload = workload
    signal.expires_at = "2026-07-26T18:00:00Z"
    signal.detail = "healthy"
    signal.signal = "ready"
    signal.target = "gameserver"
    result.server = server
    result.signals = {signal}
    result.request_id = "status-123"
  else
    opaque.assignment = "match-17"
    opaque.region = "us-central"
    workload.id = "server-42"
    workload.version = "contracts.v1"
    server.workload = workload
    server.generation = 8
    server.opaque = opaque
    signal.target = "gameserver"
    signal.signal = "ready"
    signal.detail = "healthy"
    signal.expires_at = "2026-07-26T18:00:00Z"
    result.request_id = "status-123"
    result.signals = {signal}
    result.server = server
  end
  return result
end

pulp.on("status_fingerprint", function(payload)
  return pulp.pack(status_payload(payload.reverse))
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	pack := func(reverse bool) []byte {
		t.Helper()
		result, err := runtime.Dispatch(DispatchRequest{
			Event:   "status_fingerprint",
			Payload: map[string]any{"reverse": reverse},
		})
		if err != nil {
			t.Fatalf("Dispatch reverse=%v: %v", reverse, err)
		}
		return []byte(result.Value.(string))
	}
	forward := pack(false)
	reverse := pack(true)
	if !bytes.Equal(forward, reverse) {
		t.Fatalf("semantically identical status payloads differ:\nforward %x\nreverse %x", forward, reverse)
	}
	if sha256.Sum256(forward) != sha256.Sum256(reverse) {
		t.Fatal("replayed status payload fingerprints differ")
	}
}

func TestRuntimePackRejectsUnsupportedObjectKeyTypes(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("numeric", function()
  return pulp.pack({ [1] = "array-like", label = "object" })
end)
pulp.on("boolean", function()
  return pulp.pack({ [true] = "value" })
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	for _, event := range []string{"numeric", "boolean"} {
		_, err := runtime.Dispatch(DispatchRequest{Event: event})
		if err == nil || !strings.Contains(err.Error(), "object key") || !strings.Contains(err.Error(), "is not a string") {
			t.Fatalf("%s key error = %v", event, err)
		}
	}
}

func TestRuntimeSHA256HashesExactOpaqueBytes(t *testing.T) {
	runtime, err := New(Options{Script: `
pulp.on("digest", function(payload)
  local packed = pulp.pack(payload.value)
  return {
    packed = packed,
    digest = pulp.sha256(packed),
  }
end)
`})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer runtime.Close()

	result, err := runtime.Dispatch(DispatchRequest{
		Event: "digest",
		Payload: map[string]any{
			"value": map[string]any{"workload": "server-42", "generation": int64(7)},
		},
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	value, ok := result.Value.(map[string]any)
	if !ok {
		t.Fatalf("digest result = %#v", result.Value)
	}
	packed, ok := value["packed"].(string)
	if !ok {
		t.Fatalf("packed result = %#v", value["packed"])
	}
	sum := sha256.Sum256([]byte(packed))
	if got, ok := value["digest"].(string); !ok || got != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest = %#v, want %s", value["digest"], hex.EncodeToString(sum[:]))
	}
}
