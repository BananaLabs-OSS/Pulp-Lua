package orchestrator

import (
	"fmt"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
	lua "github.com/yuin/gopher-lua"
)

type sagaRecord struct {
	requestID string
	result    workflow.SagaResult
}

// ExecuteSaga runs one versioned Lua workflow and returns only a validated
// durable outcome. The caller must use the same request and idempotency IDs on
// retries; the runtime returns the original result rather than replaying Lua
// sequencing. Durable cross-restart idempotency remains the state owner's
// responsibility, represented by the request and effect keys on the wire.
func (r *Runtime) ExecuteSaga(request workflow.SagaRequest) (workflow.SagaResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lua == nil {
		return workflow.SagaResult{}, fmt.Errorf("Lua runtime is closed")
	}
	if err := request.Validate(); err != nil {
		return workflow.SagaResult{}, err
	}
	key := request.Name + "\x00" + request.IdempotencyKey
	if record, ok := r.sagas[key]; ok {
		if record.requestID != request.RequestID {
			return workflow.SagaResult{}, fmt.Errorf("saga idempotency key is already bound to request %q", record.requestID)
		}
		return record.result, nil
	}
	handler := r.handlers[request.Name]
	if handler == nil {
		handler = r.handlers["*"]
	}
	if handler == nil {
		return workflow.SagaResult{}, fmt.Errorf("no Lua handler for saga %q", request.Name)
	}

	var decoded any
	if err := msgpack.Unmarshal(request.Payload, &decoded); err != nil {
		return workflow.SagaResult{}, fmt.Errorf("decode saga payload: %w", err)
	}
	payload, err := goToLua(r.lua, decoded, 0)
	if err != nil {
		return workflow.SagaResult{}, fmt.Errorf("lower saga payload: %w", err)
	}

	var returned lua.LValue
	// Lua receives only this read-only identity while the handler is active.
	// Restore rather than blindly clear so a future trusted nested executor
	// cannot accidentally leak or erase its caller's context.
	previousSaga := r.currentSaga
	r.currentSaga = &request
	defer func() { r.currentSaga = previousSaga }()
	err = r.runWithTimeout(func() error {
		if err := r.lua.CallByParam(lua.P{
			Fn: handler, NRet: 1, Protect: true,
		}, payload); err != nil {
			return err
		}
		returned = r.lua.Get(-1)
		r.lua.Pop(1)
		return nil
	})
	if err != nil {
		return workflow.SagaResult{}, fmt.Errorf("handle saga %q: %w", request.Name, err)
	}
	result, err := sagaResultFromLua(returned, request)
	if err != nil {
		return workflow.SagaResult{}, fmt.Errorf("decode saga %q result: %w", request.Name, err)
	}
	if err := result.Validate(); err != nil {
		return workflow.SagaResult{}, fmt.Errorf("validate saga %q result: %w", request.Name, err)
	}
	r.sagas[key] = sagaRecord{requestID: request.RequestID, result: result}
	return result, nil
}

func sagaResultFromLua(value lua.LValue, request workflow.SagaRequest) (workflow.SagaResult, error) {
	// A cross-application saga may return the canonical remote SagaResult wire
	// directly. Keeping it opaque preserves nested MessagePack RawMessage
	// fields byte-for-byte; unpacking through generic Lua values would strip
	// one encoding layer from result/effect payloads.
	if wire, ok := value.(lua.LString); ok {
		var forwarded workflow.SagaResult
		if err := msgpack.Unmarshal([]byte(string(wire)), &forwarded); err != nil {
			return workflow.SagaResult{}, fmt.Errorf("decode forwarded saga result: %w", err)
		}
		if forwarded.Version != request.Version ||
			forwarded.Name != request.Name ||
			forwarded.RequestID != request.RequestID ||
			forwarded.IdempotencyKey != request.IdempotencyKey {
			return workflow.SagaResult{}, fmt.Errorf("forwarded saga result identity does not match current request")
		}
		return forwarded, nil
	}

	table, ok := value.(*lua.LTable)
	if !ok {
		return workflow.SagaResult{}, fmt.Errorf("handler must return a table, got %s", value.Type())
	}
	status, err := requiredTableString(table, "status")
	if err != nil {
		return workflow.SagaResult{}, err
	}
	result := workflow.SagaResult{
		Version:        request.Version,
		Name:           request.Name,
		RequestID:      request.RequestID,
		IdempotencyKey: request.IdempotencyKey,
		Status:         workflow.SagaStatus(status),
		Result:         tableRawMessage(table, "result"),
	}
	if errorValue := table.RawGetString("error"); errorValue != lua.LNil {
		failure, err := sagaErrorFromLua(errorValue)
		if err != nil {
			return workflow.SagaResult{}, fmt.Errorf("error: %w", err)
		}
		result.Error = failure
	}
	effectsValue := table.RawGetString("effects")
	if effectsValue != lua.LNil {
		effects, err := effectsFromLua(effectsValue)
		if err != nil {
			return workflow.SagaResult{}, err
		}
		result.Effects = effects
	}
	return result, nil
}

func effectsFromLua(value lua.LValue) ([]workflow.EffectIntent, error) {
	table, ok := value.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("effects must be a table, got %s", value.Type())
	}
	effects := make([]workflow.EffectIntent, 0, table.Len())
	for i := 1; i <= table.Len(); i++ {
		effectTable, ok := table.RawGetInt(i).(*lua.LTable)
		if !ok {
			return nil, fmt.Errorf("effects[%d] must be a table", i)
		}
		id, err := requiredTableString(effectTable, "id")
		if err != nil {
			return nil, fmt.Errorf("effects[%d].%w", i, err)
		}
		kind, err := requiredTableString(effectTable, "kind")
		if err != nil {
			return nil, fmt.Errorf("effects[%d].%w", i, err)
		}
		key, err := requiredTableString(effectTable, "idempotency_key")
		if err != nil {
			return nil, fmt.Errorf("effects[%d].%w", i, err)
		}
		ackValue := effectTable.RawGetString("acknowledgement")
		ack, err := acknowledgementFromLua(ackValue)
		if err != nil {
			return nil, fmt.Errorf("effects[%d].acknowledgement: %w", i, err)
		}
		effects = append(effects, workflow.EffectIntent{
			ID: id, Kind: kind, IdempotencyKey: key,
			Payload: tableRawMessage(effectTable, "payload"), Acknowledgement: ack,
		})
	}
	return effects, nil
}

func acknowledgementFromLua(value lua.LValue) (workflow.EffectAcknowledgement, error) {
	table, ok := value.(*lua.LTable)
	if !ok {
		return workflow.EffectAcknowledgement{}, fmt.Errorf("must be a table")
	}
	status, err := requiredTableString(table, "status")
	if err != nil {
		return workflow.EffectAcknowledgement{}, err
	}
	ack := workflow.EffectAcknowledgement{
		Status: workflow.EffectAcknowledgementStatus(status),
		Result: tableRawMessage(table, "result"),
	}
	if errorValue := table.RawGetString("error"); errorValue != lua.LNil {
		failure, err := sagaErrorFromLua(errorValue)
		if err != nil {
			return workflow.EffectAcknowledgement{}, fmt.Errorf("error: %w", err)
		}
		ack.Error = failure
	}
	return ack, nil
}

func sagaErrorFromLua(value lua.LValue) (*workflow.SagaError, error) {
	table, ok := value.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("must be a table")
	}
	code, err := requiredTableString(table, "code")
	if err != nil {
		return nil, err
	}
	message, err := requiredTableString(table, "message")
	if err != nil {
		return nil, err
	}
	return &workflow.SagaError{Code: code, Message: message}, nil
}

func requiredTableString(table *lua.LTable, key string) (string, error) {
	value := table.RawGetString(key)
	stringValue, ok := value.(lua.LString)
	if !ok || string(stringValue) == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return string(stringValue), nil
}

func tableRawMessage(table *lua.LTable, key string) msgpack.RawMessage {
	value, ok := table.RawGetString(key).(lua.LString)
	if !ok {
		return nil
	}
	return msgpack.RawMessage([]byte(string(value)))
}
