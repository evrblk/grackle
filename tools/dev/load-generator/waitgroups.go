package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
)

// WaitGroupState tracks the load generator's coherent view of one wait group
// so it is exercised correctly:
//
//   - Jobs are drawn from a fixed [0, counter) ID space and completed with
//     monotonically increasing IDs, so completions are idempotent and the group
//     is never completed beyond its counter.
//   - The counter is only ever raised (via UpdateWaitGroup), and only while the
//     group is active — a finished group is never modified.
//   - When the group finishes (its counter is reached) it is recreated so the
//     load keeps flowing.
//
// All mutable fields are guarded by mu. Completions take the lock only briefly
// to reserve a job range; updates and recreation hold it across their RPCs so
// that only one of them touches the group at a time (which keeps the optimistic
// version in sync and avoids spurious version conflicts).
type WaitGroupState struct {
	namespace string
	name      string

	config *Config

	mu         sync.Mutex
	counter    int64
	nextJob    int64 // next job index to reserve for completion
	version    int64
	finished   bool
	recreating bool
}

// NewWaitGroupState builds an (uncreated) wait group state.
func NewWaitGroupState(namespace, name string, counter int64, config *Config) *WaitGroupState {
	return &WaitGroupState{
		namespace: namespace,
		name:      name,
		counter:   counter,
		config:    config,
	}
}

// Name returns the wait group name.
func (s *WaitGroupState) Name() string { return s.name }

func (s *WaitGroupState) expiresAt() int64 {
	return time.Now().Add(s.config.WaitGroupExpiresIn).UnixNano()
}

func (s *WaitGroupState) deleteAfterFinishedSeconds() int64 {
	return int64(s.config.WaitGroupDeleteAfterFinished.Seconds())
}

// Create creates the wait group on the server and records its initial version.
func (s *WaitGroupState) Create(ctx context.Context, client grackle.GrackleApi) error {
	resp, err := client.CreateWaitGroup(ctx, &grackle.CreateWaitGroupRequest{
		NamespaceName:              s.namespace,
		WaitGroupName:              s.name,
		Counter:                    s.counter,
		ExpiresAt:                  s.expiresAt(),
		DeleteAfterFinishedSeconds: s.deleteAfterFinishedSeconds(),
	})
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextJob = 0
	s.finished = false
	if resp.WaitGroup != nil {
		s.counter = resp.WaitGroup.Counter
		s.version = resp.WaitGroup.Version
	}
	return nil
}

// CompleteBatch completes the next batch of jobs. When the group has finished
// it recreates it instead. When all job IDs have already been reserved (but the
// group has not yet observed completion) the call is a no-op so the generator
// does not complete beyond the counter.
func (s *WaitGroupState) CompleteBatch(ctx context.Context, client grackle.GrackleApi, batchSize int) error {
	s.mu.Lock()
	if s.recreating {
		s.mu.Unlock()
		return nil
	}
	if s.finished {
		s.mu.Unlock()
		return s.recreate(ctx, client)
	}
	if s.nextJob >= s.counter {
		// All jobs have been dispatched; wait for completion to be observed
		// rather than risk overflowing the counter.
		s.mu.Unlock()
		return nil
	}

	start := s.nextJob
	end := start + int64(batchSize)
	if end > s.counter {
		end = s.counter
	}
	s.nextJob = end
	name := s.name
	s.mu.Unlock()

	jobs := make([]*grackle.CompleteJobRequest, 0, end-start)
	for id := start; id < end; id++ {
		jobs = append(jobs, &grackle.CompleteJobRequest{
			JobId: fmt.Sprintf("job-%d", id),
		})
	}

	resp, err := client.CompleteJobsFromWaitGroup(ctx, &grackle.CompleteJobsFromWaitGroupRequest{
		NamespaceName: s.namespace,
		WaitGroupName: name,
		Jobs:          jobs,
	})
	if err != nil {
		return err
	}

	if resp.WaitGroup != nil && resp.WaitGroup.Status == grackle.WaitGroupStatus_WAIT_GROUP_STATUS_COMPLETED {
		s.mu.Lock()
		s.finished = true
		s.mu.Unlock()
	}
	return nil
}

// RaiseCounter raises the counter of an active wait group by delta. The state
// lock is held across the RPC so concurrent updates do not race on the version.
func (s *WaitGroupState) RaiseCounter(ctx context.Context, client grackle.GrackleApi, delta int64, config *Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recreating || s.finished {
		return nil
	}

	newCounter := s.counter + delta
	resp, err := client.UpdateWaitGroup(ctx, &grackle.UpdateWaitGroupRequest{
		NamespaceName:              s.namespace,
		WaitGroupName:              s.name,
		Counter:                    newCounter,
		ExpiresAt:                  s.expiresAt(),
		DeleteAfterFinishedSeconds: s.deleteAfterFinishedSeconds(),
		ExpectedVersion:            s.version,
	})
	if err != nil {
		return err
	}
	if resp.WaitGroup != nil {
		s.counter = resp.WaitGroup.Counter
		s.version = resp.WaitGroup.Version
		s.finished = resp.WaitGroup.Status != grackle.WaitGroupStatus_WAIT_GROUP_STATUS_ACTIVE
	}
	return nil
}

// recreate deletes a finished wait group and creates a fresh one with the same
// name and counter, resetting the job cursor. Exactly one caller performs the
// recreation; concurrent callers see recreating and return.
func (s *WaitGroupState) recreate(ctx context.Context, client grackle.GrackleApi) error {
	s.mu.Lock()
	if s.recreating || !s.finished {
		s.mu.Unlock()
		return nil
	}
	s.recreating = true
	name := s.name
	s.mu.Unlock()

	// Best-effort delete: the group may already have been GC'd after finishing.
	_, _ = client.DeleteWaitGroup(ctx, &grackle.DeleteWaitGroupRequest{
		NamespaceName: s.namespace,
		WaitGroupName: name,
	})

	err := s.Create(ctx, client)

	s.mu.Lock()
	s.recreating = false
	s.mu.Unlock()
	return err
}
