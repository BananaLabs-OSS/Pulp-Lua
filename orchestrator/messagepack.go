package orchestrator

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

// marshalCanonicalMessagePack encodes string-keyed maps in lexical key order.
// The encoder carries this setting into nested values, so every map produced
// from a Lua object has stable bytes independent of Lua or Go map iteration.
func marshalCanonicalMessagePack(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := msgpack.NewEncoder(&buffer)
	encoder.SetSortMapKeys(true)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
