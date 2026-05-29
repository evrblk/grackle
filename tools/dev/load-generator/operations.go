package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/preview"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OperationType represents the type of operation
type OperationType int

const (
	OpAcquireLock OperationType = iota
	OpReleaseLock
	OpGetLock
	OpAcquireSemaphore
	OpReleaseSemaphore
	OpGetSemaphore
	OpAddWaitGroupJobs
	OpCompleteWaitGroupJobs
	OpGetWaitGroup
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
	case OpAcquireSemaphore:
		return "acquire_semaphore"
	case OpReleaseSemaphore:
		return "release_semaphore"
	case OpGetSemaphore:
		return "get_semaphore"
	case OpAddWaitGroupJobs:
		return "add_waitgroup_jobs"
	case OpCompleteWaitGroupJobs:
		return "complete_waitgroup_jobs"
	case OpGetWaitGroup:
		return "get_waitgroup"
	default:
		return "unknown"
	}
}

// executeAcquireLock executes a lock acquisition
func executeAcquireLock(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random lock
	locks := pool.locks[ns]
	if len(locks) == 0 {
		return fmt.Errorf("no locks available in namespace %s", ns)
	}
	lockName := locks[rng.Intn(len(locks))]

	// Generate lease ID
	leaseID := fmt.Sprintf("lease-worker-%d-%d", workerID, time.Now().UnixNano())

	// Determine if exclusive or shared
	exclusive := rng.Intn(100) < config.ExclusiveLockPct

	resp, err := client.AcquireLock(ctx, &grackle.AcquireLockRequest{
		NamespaceName: ns,
		LockName:      lockName,
		LeaseId:       leaseID,
		Exclusive:     exclusive,
	})
	if err != nil {
		return err
	}

	// Track if acquisition was successful
	if resp.Success {
		pool.TrackAcquiredLock(workerID, LockHandle{
			Namespace: ns,
			LockName:  lockName,
			LeaseID:   leaseID,
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
		// No locks to release, try to acquire one instead
		return executeAcquireLock(ctx, client, pool, workerID, rng, config)
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
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random lock
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

// executeAcquireSemaphore executes a semaphore acquisition
func executeAcquireSemaphore(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random semaphore
	semaphores := pool.semaphores[ns]
	if len(semaphores) == 0 {
		return fmt.Errorf("no semaphores available in namespace %s", ns)
	}
	semName := semaphores[rng.Intn(len(semaphores))]

	// Generate lease ID
	leaseID := fmt.Sprintf("lease-worker-%d-%d", workerID, time.Now().UnixNano())

	// Random weight (1 to max)
	weight := uint64(rng.Intn(config.SemaphoreWeightMax) + 1)

	resp, err := client.AcquireSemaphore(ctx, &grackle.AcquireSemaphoreRequest{
		NamespaceName: ns,
		SemaphoreName: semName,
		LeaseId:       leaseID,
		Weight:        weight,
	})
	if err != nil {
		return err
	}

	// Track if acquisition was successful
	if resp.Success {
		pool.TrackAcquiredSemaphore(workerID, SemaphoreHandle{
			Namespace:     ns,
			SemaphoreName: semName,
			LeaseID:       leaseID,
			Weight:        weight,
		})
	}

	return nil
}

// executeReleaseSemaphore executes a semaphore release
func executeReleaseSemaphore(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get an acquired semaphore to release
	handle := pool.GetAndRemoveAcquiredSemaphore(workerID)
	if handle == nil {
		// No semaphores to release, try to acquire one instead
		return executeAcquireSemaphore(ctx, client, pool, workerID, rng, config)
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
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random semaphore
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

// executeAddWaitGroupJobs executes adding jobs to a wait group
func executeAddWaitGroupJobs(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random wait group
	waitgroups := pool.waitGroups[ns]
	if len(waitgroups) == 0 {
		return fmt.Errorf("no wait groups available in namespace %s", ns)
	}
	wgName := waitgroups[rng.Intn(len(waitgroups))]

	// Random number of jobs to add (1 to batch size)
	counter := uint64(rng.Intn(config.WaitGroupJobBatchSize) + 1)

	_, err := client.AddJobsToWaitGroup(ctx, &grackle.AddJobsToWaitGroupRequest{
		NamespaceName: ns,
		WaitGroupName: wgName,
		Counter:       counter,
	})
	return err
}

// executeCompleteWaitGroupJobs executes completing jobs from a wait group
func executeCompleteWaitGroupJobs(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random wait group
	waitgroups := pool.waitGroups[ns]
	if len(waitgroups) == 0 {
		return fmt.Errorf("no wait groups available in namespace %s", ns)
	}
	wgName := waitgroups[rng.Intn(len(waitgroups))]

	// Random number of process IDs (1 to batch size)
	numProcesses := rng.Intn(config.WaitGroupJobBatchSize) + 1
	processIDs := make([]string, numProcesses)
	for i := range numProcesses {
		processIDs[i] = fmt.Sprintf("load-worker-%d-%d-%d", workerID, time.Now().UnixNano(), i)
	}

	_, err := client.CompleteJobsFromWaitGroup(ctx, &grackle.CompleteJobsFromWaitGroupRequest{
		NamespaceName: ns,
		WaitGroupName: wgName,
		ProcessIds:    processIDs,
	})
	return err
}

// executeGetWaitGroup executes a wait group state read
func executeGetWaitGroup(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Pick random wait group
	waitgroups := pool.waitGroups[ns]
	if len(waitgroups) == 0 {
		return fmt.Errorf("no wait groups available in namespace %s", ns)
	}
	wgName := waitgroups[rng.Intn(len(waitgroups))]

	_, err := client.GetWaitGroup(ctx, &grackle.GetWaitGroupRequest{
		NamespaceName: ns,
		WaitGroupName: wgName,
	})
	return err
}

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
