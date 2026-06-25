package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OperationType represents the type of operation
type OperationType int

const (
	// Locks
	OpAcquireLock OperationType = iota
	OpReleaseLock
	OpGetLock
	OpListLocks
	OpCreateLockLease

	// Semaphores
	OpAcquireSemaphore
	OpReleaseSemaphore
	OpGetSemaphore
	OpListSemaphores
	OpCreateSemaphoreLease

	// Wait groups
	OpCompleteWaitGroupJobs
	OpUpdateWaitGroup
	OpWaitForWaitGroup
	OpGetWaitGroup
	OpListWaitGroups

	// Barriers
	OpArriveAtBarrier
	OpWaitAtBarrier
	OpUpdateBarrier
	OpGetBarrier
	OpListBarriers
)

// String returns the string representation of an operation type
func (o OperationType) String() string {
	switch o {
	case OpAcquireLock:
		return "acquire_lock"
	case OpReleaseLock:
		return "release_lock"
	case OpGetLock:
		return "get_lock"
	case OpListLocks:
		return "list_locks"
	case OpCreateLockLease:
		return "create_lock_lease"
	case OpAcquireSemaphore:
		return "acquire_semaphore"
	case OpReleaseSemaphore:
		return "release_semaphore"
	case OpGetSemaphore:
		return "get_semaphore"
	case OpListSemaphores:
		return "list_semaphores"
	case OpCreateSemaphoreLease:
		return "create_semaphore_lease"
	case OpCompleteWaitGroupJobs:
		return "complete_waitgroup_jobs"
	case OpUpdateWaitGroup:
		return "update_waitgroup"
	case OpWaitForWaitGroup:
		return "wait_for_waitgroup"
	case OpGetWaitGroup:
		return "get_waitgroup"
	case OpListWaitGroups:
		return "list_waitgroups"
	case OpArriveAtBarrier:
		return "arrive_at_barrier"
	case OpWaitAtBarrier:
		return "wait_at_barrier"
	case OpUpdateBarrier:
		return "update_barrier"
	case OpGetBarrier:
		return "get_barrier"
	case OpListBarriers:
		return "list_barriers"
	default:
		return "unknown"
	}
}

// isBlocking reports whether an operation may block server-side for up to its
// configured timeout. Blocking operations are dispatched onto background
// goroutines so they never stall a worker's load-generation loop.
func (o OperationType) isBlocking() bool {
	switch o {
	case OpAcquireLock, OpAcquireSemaphore, OpWaitForWaitGroup, OpWaitAtBarrier:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------------

// executeAcquireLock executes a (blocking) lock acquisition. It blocks
// server-side for up to AcquireTimeout while an incompatible holder releases.
func executeAcquireLock(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get a lock lease for this worker
	lease := pool.GetRandomLease(workerID, "lock", rng)
	if lease == nil {
		// No leases available, create one first
		return executeCreateLockLease(ctx, client, pool, workerID, rng, config)
	}

	// Pick random lock in the same namespace as the lease
	locks := pool.locks[lease.Namespace]
	if len(locks) == 0 {
		return fmt.Errorf("no locks available in namespace %s", lease.Namespace)
	}
	lockName := locks[rng.Intn(len(locks))]

	// Determine if exclusive or shared
	exclusive := rng.Intn(100) < config.ExclusiveLockPct

	ctx, cancel := context.WithTimeout(ctx, config.acquireCtxTimeout())
	defer cancel()

	resp, err := client.AcquireLock(ctx, &grackle.AcquireLockRequest{
		NamespaceName:  lease.Namespace,
		LockName:       lockName,
		LeaseId:        lease.LeaseID,
		Exclusive:      exclusive,
		TimeoutSeconds: config.AcquireTimeoutSeconds(),
	})
	if err != nil {
		return err
	}

	// A non-ACQUIRED outcome (lost the race for the timeout window) is a normal
	// result, not an error — only track holders we actually acquired.
	if resp.Outcome == grackle.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED {
		pool.TrackAcquiredLock(workerID, LockHandle{
			Namespace: lease.Namespace,
			LockName:  lockName,
			LeaseID:   lease.LeaseID,
			Exclusive: exclusive,
		})
	}

	return nil
}

// executeReleaseLock executes a lock release
func executeReleaseLock(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get an acquired lock to release
	handle := pool.GetAndRemoveAcquiredLock(workerID)
	if handle == nil {
		// Nothing held to release; do a cheap read instead so the op still
		// generates load.
		return executeGetLock(ctx, client, pool, workerID, rng, config)
	}

	_, err := client.ReleaseLock(ctx, &grackle.ReleaseLockRequest{
		NamespaceName: handle.Namespace,
		LockName:      handle.LockName,
		LeaseId:       handle.LeaseID,
	})
	return err
}

// executeGetLock executes a lock state read
func executeGetLock(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	locks := pool.locks[ns]
	if len(locks) == 0 {
		return fmt.Errorf("no locks available in namespace %s", ns)
	}
	lockName := locks[rng.Intn(len(locks))]

	_, err := client.GetLock(ctx, &grackle.GetLockRequest{
		NamespaceName: ns,
		LockName:      lockName,
	})
	return err
}

// executeListLocks lists locks in a namespace
func executeListLocks(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	_, err := client.ListLocks(ctx, &grackle.ListLocksRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// executeCreateLockLease creates a new lock lease
func executeCreateLockLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	processID := fmt.Sprintf("load-worker-%d", workerID)
	ttl := config.LeaseTTL

	resp, err := client.CreateLockLease(ctx, &grackle.CreateLockLeaseRequest{
		NamespaceName: ns,
		ProcessId:     processID,
		TtlSeconds:    int64(ttl.Seconds()),
	})
	if err != nil {
		return err
	}

	pool.TrackLease(workerID, LeaseHandle{
		Namespace: ns,
		LeaseID:   resp.Lease.LeaseId,
		CreatedAt: time.Now(),
		TTL:       ttl,
		Type:      "lock",
	})
	return nil
}

// ---------------------------------------------------------------------------
// Semaphores
// ---------------------------------------------------------------------------

// executeAcquireSemaphore executes a (blocking) semaphore acquisition. It
// blocks server-side for up to AcquireTimeout while permits free up.
func executeAcquireSemaphore(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	lease := pool.GetRandomLease(workerID, "semaphore", rng)
	if lease == nil {
		return executeCreateSemaphoreLease(ctx, client, pool, workerID, rng, config)
	}

	semaphores := pool.semaphores[lease.Namespace]
	if len(semaphores) == 0 {
		return fmt.Errorf("no semaphores available in namespace %s", lease.Namespace)
	}
	semName := semaphores[rng.Intn(len(semaphores))]

	// Random weight (1 to max). Max is validated to be <= permits.
	weight := int64(rng.Intn(config.SemaphoreWeightMax) + 1)

	ctx, cancel := context.WithTimeout(ctx, config.acquireCtxTimeout())
	defer cancel()

	resp, err := client.AcquireSemaphore(ctx, &grackle.AcquireSemaphoreRequest{
		NamespaceName:  lease.Namespace,
		SemaphoreName:  semName,
		LeaseId:        lease.LeaseID,
		Weight:         weight,
		TimeoutSeconds: config.AcquireTimeoutSeconds(),
	})
	if err != nil {
		return err
	}

	if resp.Outcome == grackle.AcquireOutcome_ACQUIRE_OUTCOME_ACQUIRED {
		pool.TrackAcquiredSemaphore(workerID, SemaphoreHandle{
			Namespace:     lease.Namespace,
			SemaphoreName: semName,
			LeaseID:       lease.LeaseID,
			Weight:        weight,
		})
	}

	return nil
}

// executeReleaseSemaphore executes a semaphore release
func executeReleaseSemaphore(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	handle := pool.GetAndRemoveAcquiredSemaphore(workerID)
	if handle == nil {
		return executeGetSemaphore(ctx, client, pool, workerID, rng, config)
	}

	_, err := client.ReleaseSemaphore(ctx, &grackle.ReleaseSemaphoreRequest{
		NamespaceName: handle.Namespace,
		SemaphoreName: handle.SemaphoreName,
		LeaseId:       handle.LeaseID,
	})
	return err
}

// executeGetSemaphore executes a semaphore state read
func executeGetSemaphore(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	semaphores := pool.semaphores[ns]
	if len(semaphores) == 0 {
		return fmt.Errorf("no semaphores available in namespace %s", ns)
	}
	semName := semaphores[rng.Intn(len(semaphores))]

	_, err := client.GetSemaphore(ctx, &grackle.GetSemaphoreRequest{
		NamespaceName: ns,
		SemaphoreName: semName,
	})
	return err
}

// executeListSemaphores lists semaphores in a namespace
func executeListSemaphores(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	_, err := client.ListSemaphores(ctx, &grackle.ListSemaphoresRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// executeCreateSemaphoreLease creates a new semaphore lease
func executeCreateSemaphoreLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	processID := fmt.Sprintf("load-worker-%d", workerID)
	ttl := config.LeaseTTL

	resp, err := client.CreateSemaphoreLease(ctx, &grackle.CreateSemaphoreLeaseRequest{
		NamespaceName: ns,
		ProcessId:     processID,
		TtlSeconds:    int64(ttl.Seconds()),
	})
	if err != nil {
		return err
	}

	pool.TrackLease(workerID, LeaseHandle{
		Namespace: ns,
		LeaseID:   resp.Lease.LeaseId,
		CreatedAt: time.Now(),
		TTL:       ttl,
		Type:      "semaphore",
	})
	return nil
}

// ---------------------------------------------------------------------------
// Wait groups
// ---------------------------------------------------------------------------

// executeCompleteWaitGroupJobs completes a batch of jobs on a random wait
// group. Job IDs are drawn from the group's fixed [0, counter) job space, so
// the group is never completed beyond its counter; once it finishes it is
// recreated.
func executeCompleteWaitGroupJobs(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	wg := pool.GetRandomWaitGroup(rng)
	if wg == nil {
		return fmt.Errorf("no wait groups available")
	}
	return wg.CompleteBatch(ctx, client, config.WaitGroupJobBatchSize)
}

// executeUpdateWaitGroup raises the counter of a random active wait group.
// Finished groups are never updated.
func executeUpdateWaitGroup(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	wg := pool.GetRandomWaitGroup(rng)
	if wg == nil {
		return fmt.Errorf("no wait groups available")
	}
	return wg.RaiseCounter(ctx, client, int64(config.WaitGroupJobBatchSize), config)
}

// executeWaitForWaitGroup blocks until a random wait group completes or the
// wait timeout elapses.
func executeWaitForWaitGroup(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	wg := pool.GetRandomWaitGroup(rng)
	if wg == nil {
		return fmt.Errorf("no wait groups available")
	}

	ctx, cancel := context.WithTimeout(ctx, config.waitCtxTimeout())
	defer cancel()

	_, err := client.WaitForWaitGroup(ctx, &grackle.WaitForWaitGroupRequest{
		NamespaceName:  wg.namespace,
		WaitGroupName:  wg.Name(),
		TimeoutSeconds: config.WaitTimeoutSeconds(),
	})
	return err
}

// executeGetWaitGroup executes a wait group state read
func executeGetWaitGroup(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	wg := pool.GetRandomWaitGroup(rng)
	if wg == nil {
		return fmt.Errorf("no wait groups available")
	}
	_, err := client.GetWaitGroup(ctx, &grackle.GetWaitGroupRequest{
		NamespaceName: wg.namespace,
		WaitGroupName: wg.Name(),
	})
	return err
}

// executeListWaitGroups lists wait groups in a namespace
func executeListWaitGroups(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	_, err := client.ListWaitGroups(ctx, &grackle.ListWaitGroupsRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// ---------------------------------------------------------------------------
// Barriers
// ---------------------------------------------------------------------------

// executeArriveAtBarrier records the arrival of one logical participant at a
// random barrier. Non-blocking and idempotent per (generation, process_id).
func executeArriveAtBarrier(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	b := pool.GetRandomBarrier(rng)
	if b == nil {
		return fmt.Errorf("no barriers available")
	}
	return b.Arrive(ctx, client)
}

// executeWaitAtBarrier blocks until a random barrier releases for the current
// generation or the wait timeout elapses.
func executeWaitAtBarrier(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	b := pool.GetRandomBarrier(rng)
	if b == nil {
		return fmt.Errorf("no barriers available")
	}

	ctx, cancel := context.WithTimeout(ctx, config.waitCtxTimeout())
	defer cancel()

	return b.Wait(ctx, client, config.WaitTimeoutSeconds())
}

// executeUpdateBarrier exercises UpdateBarrier on a random barrier, keeping
// expected_processes constant so the barrier never wedges.
func executeUpdateBarrier(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	b := pool.GetRandomBarrier(rng)
	if b == nil {
		return fmt.Errorf("no barriers available")
	}
	return b.Update(ctx, client, config)
}

// executeGetBarrier executes a barrier state read
func executeGetBarrier(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	b := pool.GetRandomBarrier(rng)
	if b == nil {
		return fmt.Errorf("no barriers available")
	}
	_, err := client.GetBarrier(ctx, &grackle.GetBarrierRequest{
		NamespaceName: b.namespace,
		BarrierName:   b.Name(),
	})
	return err
}

// executeListBarriers lists barriers in a namespace
func executeListBarriers(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]
	_, err := client.ListBarriers(ctx, &grackle.ListBarriersRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// getErrorType extracts error type from gRPC error
func getErrorType(err error) string {
	if err == nil {
		return "none"
	}
	st, ok := status.FromError(err)
	if !ok {
		return "unknown"
	}
	switch st.Code() {
	case codes.OK:
		return "ok"
	case codes.NotFound:
		return "not_found"
	case codes.AlreadyExists:
		return "already_exists"
	case codes.InvalidArgument:
		return "invalid_argument"
	case codes.ResourceExhausted:
		return "resource_exhausted"
	case codes.DeadlineExceeded:
		return "deadline_exceeded"
	case codes.Canceled:
		return "canceled"
	case codes.Unavailable:
		return "unavailable"
	default:
		return st.Code().String()
	}
}
