package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPoolSlowDispatchDoesNotBlockUnrelatedLane(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	caller := contextCallFunc(func(ctx context.Context, target, function string, payload []byte) ([]byte, error) {
		if function == "slow" {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return msgpack.Marshal(function)
	})
	pool, err := NewPool(Options{Script: `
pulp.on("slow", function() return pulp.unpack(pulp.call_raw("owner", "slow", "")) end)
pulp.on("fast", function() return pulp.unpack(pulp.call_raw("owner", "fast", "")) end)
pulp.on("ordered", function(payload)
  local saga = pulp.current_saga()
  return { status = "completed", result = pulp.pack(saga.request_id) }
end)`, Caller: caller, Timeout: time.Second}, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	done := make(chan error, 1)
	go func() { _, err := pool.Dispatch(DispatchRequest{Event: "slow"}); done <- err }()
	<-started
	fastDone := make(chan error, 1)
	go func() { _, err := pool.Dispatch(DispatchRequest{Event: "fast"}); fastDone <- err }()
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("fast dispatch blocked behind slow lane")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	payload, _ := msgpack.Marshal(map[string]any{})
	request, _ := workflow.NewSagaRequest("ordered", "request-1", "key-1", map[string]any{"wire": string(payload)})
	first, err := pool.ExecuteSaga(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.ExecuteSaga(request)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Result) != string(second.Result) {
		t.Fatal("same saga retry did not retain ordered idempotent result")
	}
}

type contextCallFunc func(context.Context, string, string, []byte) ([]byte, error)

func (f contextCallFunc) Call(target, function string, payload []byte) ([]byte, error) {
	return f(context.Background(), target, function, payload)
}
func (f contextCallFunc) CallContext(ctx context.Context, target, function string, payload []byte) ([]byte, error) {
	return f(ctx, target, function, payload)
}

func TestDispatchContextCancelsProviderCall(t *testing.T) {
	caller := contextCallFunc(func(ctx context.Context, _, _ string, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	runtime, err := New(Options{Script: `pulp.on("wait", function() return pulp.call_raw("owner", "wait", "") end)`, Caller: caller, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := runtime.DispatchContext(ctx, DispatchRequest{Event: "wait"}); err == nil {
		t.Fatal("cancelled provider call succeeded")
	}
	if time.Since(started) > 300*time.Millisecond {
		t.Fatal("caller cancellation was not propagated")
	}
}
