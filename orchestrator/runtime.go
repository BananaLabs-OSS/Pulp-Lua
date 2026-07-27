// Package orchestrator embeds a capability-safe Lua application composition
// runtime. It deliberately knows nothing about the Pulp WASM ABI; the deployable
// cell supplies a Caller adapter, which also keeps this package natively
// testable.
package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
	lua "github.com/yuin/gopher-lua"
)

const FnDispatch = workflow.FnDispatch
const FnExecuteSaga = workflow.FnExecuteSaga

type Caller interface {
	Call(target, function string, payload []byte) ([]byte, error)
}

// AppCaller is the deliberately narrow cross-application call seam. The Pulp
// host, not Lua, resolves the named application instance and verifies the
// manifest-declared link before executing a provider. It is intentionally
// separate from Caller so a local sibling capability can never become an
// ambient cross-application capability by accident.
type AppCaller interface {
	AppCall(app, instance, cell, provider string, payload []byte) ([]byte, error)
}

type CallFunc func(target, function string, payload []byte) ([]byte, error)

func (f CallFunc) Call(target, function string, payload []byte) ([]byte, error) {
	return f(target, function, payload)
}

// AppCallFunc adapts a function for native tests and host adapters without
// widening the sandbox's authority surface.
type AppCallFunc func(app, instance, cell, provider string, payload []byte) ([]byte, error)

func (f AppCallFunc) AppCall(app, instance, cell, provider string, payload []byte) ([]byte, error) {
	return f(app, instance, cell, provider, payload)
}

type Options struct {
	Script    string
	Timeout   time.Duration
	Caller    Caller
	AppCaller AppCaller
	Logf      func(format string, args ...any)
}

type DispatchRequest = workflow.DispatchRequest
type Action = workflow.Action
type DispatchResult = workflow.DispatchResult
type SagaRequest = workflow.SagaRequest
type SagaResult = workflow.SagaResult

type Runtime struct {
	mu          sync.Mutex
	lua         *lua.LState
	handlers    map[string]*lua.LFunction
	state       map[string]any
	caller      Caller
	appCaller   AppCaller
	timeout     time.Duration
	logf        func(format string, args ...any)
	current     *DispatchResult
	currentSaga *workflow.SagaRequest
	sagas       map[string]sagaRecord
}

func New(options Options) (*Runtime, error) {
	if options.Script == "" {
		return nil, fmt.Errorf("Lua script is required")
	}
	if options.Timeout <= 0 {
		options.Timeout = 30 * time.Second
	}
	if options.Logf == nil {
		options.Logf = log.Printf
	}

	l := lua.NewState(lua.Options{
		CallStackSize:    128,
		RegistrySize:     2048,
		RegistryMaxSize:  8192,
		RegistryGrowStep: 256,
		SkipOpenLibs:     true,
	})
	runtime := &Runtime{
		lua:       l,
		handlers:  map[string]*lua.LFunction{},
		state:     map[string]any{},
		caller:    options.Caller,
		appCaller: options.AppCaller,
		timeout:   options.Timeout,
		logf:      options.Logf,
		sagas:     map[string]sagaRecord{},
	}
	if err := runtime.openSandbox(); err != nil {
		l.Close()
		return nil, err
	}
	runtime.installPulpModule()
	if err := runtime.runWithTimeout(func() error {
		return l.DoString(options.Script)
	}); err != nil {
		l.Close()
		return nil, fmt.Errorf("load Lua application: %w", err)
	}
	if len(runtime.handlers) == 0 {
		l.Close()
		return nil, fmt.Errorf("Lua application registered no handlers")
	}
	return runtime, nil
}

func (r *Runtime) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lua != nil {
		r.lua.Close()
		r.lua = nil
	}
}

func (r *Runtime) Dispatch(request DispatchRequest) (DispatchResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lua == nil {
		return DispatchResult{}, fmt.Errorf("Lua runtime is closed")
	}
	if err := request.Validate(); err != nil {
		return DispatchResult{}, err
	}
	handler := r.handlers[request.Event]
	if handler == nil {
		handler = r.handlers["*"]
	}
	if handler == nil {
		return DispatchResult{}, fmt.Errorf("no Lua handler for event %q", request.Event)
	}

	payload, err := goToLua(r.lua, request.Payload, 0)
	if err != nil {
		return DispatchResult{}, fmt.Errorf("lower payload: %w", err)
	}
	result := DispatchResult{}
	r.current = &result
	defer func() { r.current = nil }()

	err = r.runWithTimeout(func() error {
		if err := r.lua.CallByParam(lua.P{
			Fn:      handler,
			NRet:    1,
			Protect: true,
		}, payload); err != nil {
			return err
		}
		returned := r.lua.Get(-1)
		r.lua.Pop(1)
		if returned == lua.LNil {
			return nil
		}
		value, err := luaToGo(returned, map[*lua.LTable]bool{}, 0)
		if err != nil {
			return fmt.Errorf("lift handler result: %w", err)
		}
		result.Value = value
		return nil
	})
	if err != nil {
		return DispatchResult{}, fmt.Errorf("handle %q: %w", request.Event, err)
	}
	if err := result.Validate(); err != nil {
		return DispatchResult{}, fmt.Errorf("validate %q result: %w", request.Event, err)
	}
	return result, nil
}

func (r *Runtime) runWithTimeout(fn func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	r.lua.SetContext(ctx)
	defer r.lua.RemoveContext()
	return fn()
}

func (r *Runtime) openSandbox() error {
	libraries := []struct {
		name string
		open lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
	}
	for _, library := range libraries {
		if err := r.lua.CallByParam(lua.P{
			Fn:      r.lua.NewFunction(library.open),
			NRet:    0,
			Protect: true,
		}, lua.LString(library.name)); err != nil {
			return fmt.Errorf("open Lua library %s: %w", library.name, err)
		}
	}

	for _, name := range []string{
		"dofile", "load", "loadfile", "loadstring", "module", "require",
		"collectgarbage",
	} {
		r.lua.SetGlobal(name, lua.LNil)
	}
	if mathTable, ok := r.lua.GetGlobal(lua.MathLibName).(*lua.LTable); ok {
		mathTable.RawSetString("random", lua.LNil)
		mathTable.RawSetString("randomseed", lua.LNil)
	}
	r.lua.SetGlobal("print", r.lua.NewFunction(r.luaLog))
	return nil
}

func (r *Runtime) installPulpModule() {
	module := r.lua.SetFuncs(r.lua.NewTable(), map[string]lua.LGFunction{
		"on":               r.luaOn,
		"call":             r.luaCall,
		"call_raw":         r.luaCallRaw,
		"app_call_raw":     r.luaAppCallRaw,
		"current_saga":     r.luaCurrentSaga,
		"app_execute_saga": r.luaAppExecuteSaga,
		"pack":             r.luaPack,
		"bytes":            r.luaBytes,
		"sha256":           r.luaSHA256,
		"raw":              r.luaRaw,
		"unpack":           r.luaUnpack,
		"command":          r.luaCommand,
		"emit":             r.luaEmit,
		"state_get":        r.luaStateGet,
		"state_set":        r.luaStateSet,
		"log":              r.luaLog,
	})
	r.lua.SetGlobal("pulp", module)
}

func (r *Runtime) luaOn(l *lua.LState) int {
	event := l.CheckString(1)
	handler := l.CheckFunction(2)
	if event == "" {
		l.RaiseError("pulp.on: event is required")
		return 0
	}
	r.handlers[event] = handler
	return 0
}

func (r *Runtime) luaCall(l *lua.LState) int {
	if r.caller == nil {
		l.RaiseError("pulp.call: no sibling caller is configured")
		return 0
	}
	target := l.CheckString(1)
	function := l.CheckString(2)
	payload := l.Get(3)
	goPayload, err := luaToGo(payload, map[*lua.LTable]bool{}, 0)
	if err != nil {
		l.RaiseError("pulp.call: encode payload: %v", err)
		return 0
	}
	encoded, err := msgpack.Marshal(goPayload)
	if err != nil {
		l.RaiseError("pulp.call: encode payload: %v", err)
		return 0
	}
	response, err := r.caller.Call(target, function, encoded)
	if err != nil {
		l.RaiseError("pulp.call(%s, %s): %v", target, function, err)
		return 0
	}
	if len(response) == 0 {
		l.Push(lua.LNil)
		return 1
	}
	var decoded any
	if err := msgpack.Unmarshal(response, &decoded); err != nil {
		l.RaiseError("pulp.call: decode response: %v", err)
		return 0
	}
	value, err := goToLua(l, decoded, 0)
	if err != nil {
		l.RaiseError("pulp.call: lower response: %v", err)
		return 0
	}
	l.Push(value)
	return 1
}

func (r *Runtime) luaCallRaw(l *lua.LState) int {
	if r.caller == nil {
		l.RaiseError("pulp.call_raw: no sibling caller is configured")
		return 0
	}
	target := l.CheckString(1)
	function := l.CheckString(2)
	payload := []byte(l.OptString(3, ""))
	response, err := r.caller.Call(target, function, payload)
	if err != nil {
		l.RaiseError("pulp.call_raw(%s, %s): %v", target, function, err)
		return 0
	}
	l.Push(lua.LString(string(response)))
	return 1
}

const (
	maxAppCallNameBytes = 256
	maxAppCallPayload   = 1 << 20 // Bound guest-to-host allocation per call.
)

// luaAppCallRaw makes one explicitly addressed call to a provider in another
// application instance. There is no list, lookup, wildcard, or default-app
// operation: every address segment is supplied by the script and then checked
// by Pulp against the host manifest.
func (r *Runtime) luaAppCallRaw(l *lua.LState) int {
	if r.appCaller == nil {
		l.RaiseError("pulp.app_call_raw: no cross-application caller is configured")
		return 0
	}
	if l.GetTop() != 5 {
		l.RaiseError("pulp.app_call_raw: requires app, instance, cell, provider, payload")
		return 0
	}
	app, ok := appCallString(l, 1, "app", "pulp.app_call_raw")
	if !ok {
		return 0
	}
	instance, ok := appCallString(l, 2, "instance", "pulp.app_call_raw")
	if !ok {
		return 0
	}
	cell, ok := appCallString(l, 3, "cell", "pulp.app_call_raw")
	if !ok {
		return 0
	}
	provider, ok := appCallString(l, 4, "provider", "pulp.app_call_raw")
	if !ok {
		return 0
	}
	payloadValue := l.Get(5)
	if payloadValue.Type() != lua.LTString {
		l.RaiseError("pulp.app_call_raw: payload must be an opaque byte string")
		return 0
	}
	payload := []byte(string(payloadValue.(lua.LString)))
	if len(payload) > maxAppCallPayload {
		l.RaiseError("pulp.app_call_raw: payload exceeds %d bytes", maxAppCallPayload)
		return 0
	}
	// Never hand a host implementation an alias to Lua-owned backing memory.
	payload = append([]byte(nil), payload...)
	response, err := r.appCaller.AppCall(app, instance, cell, provider, payload)
	if err != nil {
		l.RaiseError("pulp.app_call_raw(%s, %s, %s, %s): %v", app, instance, cell, provider, err)
		return 0
	}
	if len(response) > maxAppCallPayload {
		l.RaiseError("pulp.app_call_raw: response exceeds %d bytes", maxAppCallPayload)
		return 0
	}
	l.Push(lua.LString(string(response)))
	return 1
}

func appCallString(l *lua.LState, index int, field, api string) (string, bool) {
	value := l.Get(index)
	if value.Type() != lua.LTString {
		l.RaiseError("%s: %s must be a string", api, field)
		return "", false
	}
	result := string(value.(lua.LString))
	if result == "" {
		l.RaiseError("%s: %s is required", api, field)
		return "", false
	}
	if len(result) > maxAppCallNameBytes {
		l.RaiseError("%s: %s exceeds %d bytes", api, field, maxAppCallNameBytes)
		return "", false
	}
	return result, true
}

// luaCurrentSaga exposes the original immutable workflow identity only while
// ExecuteSaga is invoking its Lua handler. It does not expose mutable host
// state or payload bytes and is cleared on every return path.
func (r *Runtime) luaCurrentSaga(l *lua.LState) int {
	if r.currentSaga == nil {
		l.RaiseError("pulp.current_saga is only valid while executing a saga")
		return 0
	}
	request := r.currentSaga
	value := l.NewTable()
	value.RawSetString("request_id", lua.LString(request.RequestID))
	value.RawSetString("idempotency_key", lua.LString(request.IdempotencyKey))
	value.RawSetString("workflow", lua.LString(request.Version))
	value.RawSetString("event", lua.LString(request.Name))
	l.Push(value)
	return 1
}

// luaAppExecuteSaga forwards the exact current SagaRequest to the standard
// orchestrator provider in another explicitly addressed application. It never
// creates IDs or idempotency keys, so retries remain one logical saga across
// application boundaries. The returned opaque bytes are a validated canonical
// MessagePack SagaResult and can be inspected with pulp.unpack.
func (r *Runtime) luaAppExecuteSaga(l *lua.LState) int {
	if r.currentSaga == nil {
		l.RaiseError("pulp.app_execute_saga is only valid while executing a saga")
		return 0
	}
	if r.appCaller == nil {
		l.RaiseError("pulp.app_execute_saga: no cross-application caller is configured")
		return 0
	}
	if l.GetTop() != 3 {
		l.RaiseError("pulp.app_execute_saga: requires app, instance, cell")
		return 0
	}
	app, ok := appCallString(l, 1, "app", "pulp.app_execute_saga")
	if !ok {
		return 0
	}
	instance, ok := appCallString(l, 2, "instance", "pulp.app_execute_saga")
	if !ok {
		return 0
	}
	cell, ok := appCallString(l, 3, "cell", "pulp.app_execute_saga")
	if !ok {
		return 0
	}
	request := *r.currentSaga
	wire, err := msgpack.Marshal(request)
	if err != nil {
		l.RaiseError("pulp.app_execute_saga: encode request: %v", err)
		return 0
	}
	response, err := r.appCaller.AppCall(app, instance, cell, workflow.FnExecuteSaga, wire)
	if err != nil {
		l.RaiseError("pulp.app_execute_saga(%s, %s, %s): %v", app, instance, cell, err)
		return 0
	}
	var result workflow.SagaResult
	if err := msgpack.Unmarshal(response, &result); err != nil {
		l.RaiseError("pulp.app_execute_saga: decode result: %v", err)
		return 0
	}
	if err := result.Validate(); err != nil {
		l.RaiseError("pulp.app_execute_saga: validate result: %v", err)
		return 0
	}
	if result.Version != request.Version || result.Name != request.Name || result.RequestID != request.RequestID || result.IdempotencyKey != request.IdempotencyKey {
		l.RaiseError("pulp.app_execute_saga: response identity does not match current saga")
		return 0
	}
	canonical, err := msgpack.Marshal(result)
	if err != nil {
		l.RaiseError("pulp.app_execute_saga: encode result: %v", err)
		return 0
	}
	l.Push(lua.LString(string(canonical)))
	return 1
}

// luaSHA256 returns the lowercase SHA-256 digest of one opaque byte string.
// It is a pure codec companion to pulp.pack: Lua gains no file, process,
// network, clock, or randomness authority through this helper.
func (r *Runtime) luaSHA256(l *lua.LState) int {
	raw := []byte(l.CheckString(1))
	sum := sha256.Sum256(raw)
	l.Push(lua.LString(hex.EncodeToString(sum[:])))
	return 1
}

// luaPack encodes a sandbox-safe Lua value into an opaque MessagePack byte
// string. Lua can pass that value to call_raw or return it in a typed saga
// result, but it gains no ambient file, process, or network authority.
func (r *Runtime) luaPack(l *lua.LState) int {
	value, err := luaToPackValue(l.Get(1), map[*lua.LTable]bool{}, 0)
	if err != nil {
		l.RaiseError("pulp.pack: encode value: %v", err)
		return 0
	}
	encoded, err := marshalCanonicalMessagePack(value)
	if err != nil {
		l.RaiseError("pulp.pack: encode value: %v", err)
		return 0
	}
	l.Push(lua.LString(string(encoded)))
	return 1
}

type rawMessageValue []byte

// bytesValue marks an opaque Lua string for MessagePack's binary type.  Lua
// strings otherwise encode as MessagePack strings, which cannot satisfy a Go
// []byte contract.  Unlike rawMessageValue this is not embedded MessagePack;
// its bytes are kept opaque by the receiving owner.
type bytesValue []byte

// luaBytes creates a private binary marker for pulp.pack.  It is deliberately
// distinct from pulp.raw: raw embeds one already-valid MessagePack value,
// whereas bytes encodes an opaque MessagePack bin field.
func (r *Runtime) luaBytes(l *lua.LState) int {
	cloned := append(bytesValue(nil), []byte(l.CheckString(1))...)
	userdata := l.NewUserData()
	userdata.Value = cloned
	metatable := l.NewTable()
	metatable.RawSetString("__metatable", lua.LString("pulp.bytes"))
	l.SetMetatable(userdata, metatable)
	l.Push(userdata)
	return 1
}

// luaRaw marks exactly one validated MessagePack value for embedding by
// pulp.pack. The private userdata is rejected everywhere else and carries a
// cloned byte slice, so Lua cannot mutate or retain host-owned memory.
func (r *Runtime) luaRaw(l *lua.LState) int {
	raw := []byte(l.CheckString(1))
	var decoded any
	if err := msgpack.Unmarshal(raw, &decoded); err != nil {
		l.RaiseError("pulp.raw: invalid MessagePack value: %v", err)
		return 0
	}
	cloned := append(rawMessageValue(nil), raw...)
	userdata := l.NewUserData()
	userdata.Value = cloned
	metatable := l.NewTable()
	metatable.RawSetString("__metatable", lua.LString("pulp.raw"))
	l.SetMetatable(userdata, metatable)
	l.Push(userdata)
	return 1
}

// luaUnpack decodes an opaque MessagePack byte string into a sandbox-safe Lua
// value. It is deliberately a pure codec helper: it does not add I/O or host
// capabilities to Lua.
func (r *Runtime) luaUnpack(l *lua.LState) int {
	raw := []byte(l.CheckString(1))
	var decoded any
	if err := msgpack.Unmarshal(raw, &decoded); err != nil {
		l.RaiseError("pulp.unpack: decode value: %v", err)
		return 0
	}
	value, err := goToLua(l, decoded, 0)
	if err != nil {
		l.RaiseError("pulp.unpack: lower value: %v", err)
		return 0
	}
	l.Push(value)
	return 1
}

func (r *Runtime) luaCommand(l *lua.LState) int {
	if r.current == nil {
		l.RaiseError("pulp.command is only valid while handling an event")
		return 0
	}
	r.appendAction(l, "command", &r.current.Commands)
	return 0
}

func (r *Runtime) luaEmit(l *lua.LState) int {
	if r.current == nil {
		l.RaiseError("pulp.emit is only valid while handling an event")
		return 0
	}
	r.appendAction(l, "emit", &r.current.Events)
	return 0
}

func (r *Runtime) appendAction(l *lua.LState, kind string, target *[]Action) {
	name := l.CheckString(1)
	if name == "" {
		l.RaiseError("pulp.%s: name is required", kind)
		return
	}
	payload, err := luaToGo(l.Get(2), map[*lua.LTable]bool{}, 0)
	if err != nil {
		l.RaiseError("pulp.%s: encode payload: %v", kind, err)
		return
	}
	*target = append(*target, Action{Name: name, Payload: payload})
}

func (r *Runtime) luaStateGet(l *lua.LState) int {
	key := l.CheckString(1)
	value, ok := r.state[key]
	if !ok {
		l.Push(lua.LNil)
		return 1
	}
	lv, err := goToLua(l, value, 0)
	if err != nil {
		l.RaiseError("pulp.state_get: %v", err)
		return 0
	}
	l.Push(lv)
	return 1
}

func (r *Runtime) luaStateSet(l *lua.LState) int {
	key := l.CheckString(1)
	if key == "" {
		l.RaiseError("pulp.state_set: key is required")
		return 0
	}
	if l.Get(2) == lua.LNil {
		delete(r.state, key)
		return 0
	}
	value, err := luaToGo(l.Get(2), map[*lua.LTable]bool{}, 0)
	if err != nil {
		l.RaiseError("pulp.state_set: %v", err)
		return 0
	}
	r.state[key] = value
	return 0
}

func (r *Runtime) luaLog(l *lua.LState) int {
	parts := make([]any, 0, l.GetTop())
	for i := 1; i <= l.GetTop(); i++ {
		parts = append(parts, l.Get(i).String())
	}
	r.logf("[lua] %v", parts)
	return 0
}

func numberToGo(number lua.LNumber) any {
	value := float64(number)
	if math.Trunc(value) == value && value >= math.MinInt64 && value <= math.MaxInt64 {
		return int64(value)
	}
	return value
}
