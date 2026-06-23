package main

import (
	"context"
	"fmt"
	"sync"

	grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
)

// BarrierState tracks the load generator's coherent view of one barrier so it
// is exercised correctly:
//
//   - The barrier expects a fixed number of participants. The generator
//     simulates that many logical participants ("p-0".."p-(N-1)") per
//     generation, so the barrier actually trips and advances generations
//     instead of wedging half-arrived.
//   - Arrivals and waits always carry the generation the generator currently
//     believes is open, reconciled from every server response so a stale
//     generation is corrected rather than retried blindly.
//
// All mutable fields are guarded by mu. Arrivals and waits take the lock only
// briefly to read/reconcile the generation; updates hold it across the RPC so
// the optimistic version stays in sync.
type BarrierState struct {
	namespace string
	name      string
	expected  uint64

	config *Config

	mu         sync.Mutex
	generation uint64
	nextIdx    uint64 // next participant index for the current generation
	version    uint64
}

// NewBarrierState builds an (uncreated) barrier state.
func NewBarrierState(namespace, name string, expected uint64, config *Config) *BarrierState {
	return &BarrierState{
		namespace:  namespace,
		name:       name,
		expected:   expected,
		generation: 1,
		config:     config,
	}
}

// Name returns the barrier name.
func (s *BarrierState) Name() string { return s.name }

func (s *BarrierState) deleteInactiveAfterSeconds() int64 {
	return int64(s.config.BarrierDeleteInactiveAfter.Seconds())
}

// Create creates the barrier on the server and records its initial state.
func (s *BarrierState) Create(ctx context.Context, client grackle.GrackleApi) error {
	resp, err := client.CreateBarrier(ctx, &grackle.CreateBarrierRequest{
		NamespaceName:              s.namespace,
		BarrierName:                s.name,
		ExpectedProcesses:          s.expected,
		DeleteInactiveAfterSeconds: s.deleteInactiveAfterSeconds(),
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if resp.Barrier != nil {
		s.generation = resp.Barrier.Generation
		s.version = resp.Barrier.Version
	}
	s.nextIdx = 0
	return nil
}

// reconcile advances the local view to match an observed barrier generation,
// resetting the participant cursor when a new generation begins. Caller holds mu.
func (s *BarrierState) reconcile(generation uint64) {
	if generation > s.generation {
		s.generation = generation
		s.nextIdx = 0
	}
}

// Arrive records the arrival of the next logical participant at the barrier's
// current generation. Idempotent per (generation, process_id) server-side.
func (s *BarrierState) Arrive(ctx context.Context, client grackle.GrackleApi) error {
	s.mu.Lock()
	generation := s.generation
	idx := s.nextIdx % s.expected
	s.nextIdx++
	s.mu.Unlock()

	resp, err := client.ArriveAtBarrier(ctx, &grackle.ArriveAtBarrierRequest{
		NamespaceName:      s.namespace,
		BarrierName:        s.name,
		ProcessId:          fmt.Sprintf("p-%d", idx),
		ExpectedGeneration: generation,
	})
	if err != nil {
		return err
	}

	if resp.Barrier != nil {
		s.mu.Lock()
		s.reconcile(resp.Barrier.Generation)
		s.mu.Unlock()
	}
	return nil
}

// Wait blocks until the barrier releases for the generation the generator
// currently believes is open, or the timeout elapses.
func (s *BarrierState) Wait(ctx context.Context, client grackle.GrackleApi, timeoutSeconds int32) error {
	s.mu.Lock()
	generation := s.generation
	s.mu.Unlock()

	resp, err := client.WaitAtBarrier(ctx, &grackle.WaitAtBarrierRequest{
		NamespaceName:      s.namespace,
		BarrierName:        s.name,
		ExpectedGeneration: generation,
		TimeoutSeconds:     timeoutSeconds,
	})
	if err != nil {
		return err
	}

	if resp.Barrier != nil {
		s.mu.Lock()
		s.reconcile(resp.Barrier.Generation)
		s.mu.Unlock()
	}
	return nil
}

// Update exercises UpdateBarrier while keeping expected_processes constant so
// the barrier never trips on update or wedges. The lock is held across the RPC
// so concurrent updates do not race on the version.
func (s *BarrierState) Update(ctx context.Context, client grackle.GrackleApi, config *Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	resp, err := client.UpdateBarrier(ctx, &grackle.UpdateBarrierRequest{
		NamespaceName:              s.namespace,
		BarrierName:                s.name,
		ExpectedProcesses:          s.expected,
		DeleteInactiveAfterSeconds: s.deleteInactiveAfterSeconds(),
		ExpectedVersion:            s.version,
	})
	if err != nil {
		return err
	}
	if resp.Barrier != nil {
		s.version = resp.Barrier.Version
		s.reconcile(resp.Barrier.Generation)
	}
	return nil
}
