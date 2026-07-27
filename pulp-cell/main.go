package main

import (
	"fmt"
	"log"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp"
	"github.com/BananaLabs-OSS/Fiber/pulp/cellconfig"
	"github.com/BananaLabs-OSS/Pulp-Lua/orchestrator"
	"github.com/vmihailenco/msgpack/v5"
)

type config struct {
	Script    string `json:"script"`
	TimeoutMS int    `json:"timeout_ms"`
}

var luaRuntime *orchestrator.Runtime

type pulpCaller struct{}

func (pulpCaller) Call(target, function string, payload []byte) ([]byte, error) {
	return pulp.Call(target, function, payload)
}

// AppCall is intentionally a separate adapter from Call: Pulp authorizes it
// through the multi-host manifest and never supplies a default application or
// instance to the Lua runtime.
func (pulpCaller) AppCall(app, instance, cell, provider string, payload []byte) ([]byte, error) {
	return pulp.AppCall(app, instance, cell, provider, payload)
}

func init() {
	pulp.OnInit(func(configBytes []byte) error {
		var cfg config
		if err := cellconfig.Decode(configBytes, &cfg); err != nil {
			return fmt.Errorf("decode config: %w", err)
		}
		timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
		runtime, err := orchestrator.New(orchestrator.Options{
			Script:    cfg.Script,
			Timeout:   timeout,
			Caller:    pulpCaller{},
			AppCaller: pulpCaller{},
			Logf:      log.Printf,
		})
		if err != nil {
			return err
		}
		luaRuntime = runtime
		pulp.Provide(orchestrator.FnDispatch, func(input []byte) ([]byte, error) {
			var request orchestrator.DispatchRequest
			if err := msgpack.Unmarshal(input, &request); err != nil {
				return nil, fmt.Errorf("decode dispatch: %w", err)
			}
			result, err := luaRuntime.Dispatch(request)
			if err != nil {
				return nil, err
			}
			return msgpack.Marshal(result)
		})
		pulp.Provide(orchestrator.FnExecuteSaga, func(input []byte) ([]byte, error) {
			var request orchestrator.SagaRequest
			if err := msgpack.Unmarshal(input, &request); err != nil {
				return nil, fmt.Errorf("decode saga: %w", err)
			}
			result, err := luaRuntime.ExecuteSaga(request)
			if err != nil {
				return nil, err
			}
			return msgpack.Marshal(result)
		})
		return nil
	})
	pulp.OnShutdown(func() error {
		if luaRuntime != nil {
			luaRuntime.Close()
			luaRuntime = nil
		}
		return nil
	})
}

func main() {}
