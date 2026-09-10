package orchestrator

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/BananaLabs-OSS/Fiber/pulp/workflow"
)

// Pool provides explicitly stateless dispatch lanes and one ordered saga lane.
// Constructing it is an application-level assertion that Dispatch handlers do
// not mutate Lua globals or use pulp.state_set; durable state must remain in
// owner cells. ExecuteSaga is always routed to the primary lane so retries and
// saga ordering retain the Runtime contract.
type Pool struct {
	primary *Runtime
	reads   []*Runtime
	next    atomic.Uint64
}

func NewPool(options Options, readLanes int) (*Pool, error) {
	if readLanes < 1 {
		return nil, fmt.Errorf("read lanes must be at least one")
	}
	primary, err := New(options)
	if err != nil {
		return nil, err
	}
	pool := &Pool{primary: primary}
	for range readLanes {
		lane, laneErr := New(options)
		if laneErr != nil {
			pool.Close()
			return nil, laneErr
		}
		pool.reads = append(pool.reads, lane)
	}
	return pool, nil
}

// Dispatch executes an application-declared stateless request on a bounded
// independent lane, preventing a slow workflow from blocking unrelated reads.
func (p *Pool) Dispatch(request DispatchRequest) (DispatchResult, error) {
	return p.DispatchContext(context.Background(), request)
}

func (p *Pool) DispatchContext(ctx context.Context, request DispatchRequest) (DispatchResult, error) {
	index := (p.next.Add(1) - 1) % uint64(len(p.reads))
	return p.reads[index].DispatchContext(ctx, request)
}

func (p *Pool) ExecuteSaga(request workflow.SagaRequest) (workflow.SagaResult, error) {
	return p.primary.ExecuteSaga(request)
}

func (p *Pool) ExecuteSagaContext(ctx context.Context, request workflow.SagaRequest) (workflow.SagaResult, error) {
	return p.primary.ExecuteSagaContext(ctx, request)
}

func (p *Pool) Close() {
	if p.primary != nil {
		p.primary.Close()
	}
	for _, lane := range p.reads {
		lane.Close()
	}
}
