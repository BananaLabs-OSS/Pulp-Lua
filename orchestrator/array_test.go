package orchestrator

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestPulpArrayPreservesAnEmptyArrayForTypedCellCalls(t *testing.T) {
	runtime, err := New(Options{
		Caller: CallFunc(func(_, _ string, payload []byte) ([]byte, error) {
			var request struct {
				Records []any `msgpack:"records"`
			}
			if err := msgpack.Unmarshal(payload, &request); err != nil {
				t.Fatal(err)
			}
			if request.Records == nil || len(request.Records) != 0 {
				t.Fatalf("records = %#v", request.Records)
			}
			return msgpack.Marshal(map[string]any{"ok": true})
		}),
		Script: `pulp.on("empty", function() return pulp.call("typed", "typed.call", { records = pulp.array({}) }) end)`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if _, err = runtime.Dispatch(DispatchRequest{Event: "empty"}); err != nil {
		t.Fatal(err)
	}
}
