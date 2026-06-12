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
	OpAcquireLock OperationType = iota
	OpReleaseLock
	OpGetLock
	OpListLocks
	OpCreateLockLease
	OpRefreshLockLease
	OpRevokeLockLease
	OpListLockLeases
	OpAcquireSemaphore
	OpReleaseSemaphore
	OpGetSemaphore
	OpListSemaphores
	OpCreateSemaphoreLease
	OpRefreshSemaphoreLease
	OpRevokeSemaphoreLease
	OpListSemaphoreLeases
	OpAddWaitGroupJobs
	OpCompleteWaitGroupJobs
	OpGetWaitGroup
	OpListWaitGroups
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
	case OpRefreshLockLease:
		return "refresh_lock_lease"
	case OpRevokeLockLease:
		return "revoke_lock_lease"
	case OpListLockLeases:
		return "list_lock_leases"
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
	case OpRefreshSemaphoreLease:
		return "refresh_semaphore_lease"
	case OpRevokeSemaphoreLease:
		return "revoke_semaphore_lease"
	case OpListSemaphoreLeases:
		return "list_semaphore_leases"
	case OpAddWaitGroupJobs:
		return "add_waitgroup_jobs"
	case OpCompleteWaitGroupJobs:
		return "complete_waitgroup_jobs"
	case OpGetWaitGroup:
		return "get_waitgroup"
	case OpListWaitGroups:
		return "list_waitgroups"
	default:
		return "unknown"
	}
}

// executeAcquireLock executes a lock acquisition
func executeAcquireLock(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get a lock lease for this worker
	lease := pool.GetRandomLease(workerID, "lock")
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

	resp, err := client.AcquireLock(ctx, &grackle.AcquireLockRequest{
		NamespaceName:  lease.Namespace,
		LockName:       lockName,
		LeaseId:        lease.LeaseID,
		Exclusive:      exclusive,
		TimeoutSeconds: 10,
	})
	if err != nil {
		return err
	}

	// Track if acquisition was successful
	if resp.Success {
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
	// Get a semaphore lease for this worker
	lease := pool.GetRandomLease(workerID, "semaphore")
	if lease == nil {
		// No leases available, create one first
		return executeCreateSemaphoreLease(ctx, client, pool, workerID, rng, config)
	}

	// Pick random semaphore in the same namespace as the lease
	semaphores := pool.semaphores[lease.Namespace]
	if len(semaphores) == 0 {
		return fmt.Errorf("no semaphores available in namespace %s", lease.Namespace)
	}
	semName := semaphores[rng.Intn(len(semaphores))]

	// Random weight (1 to max)
	weight := uint64(rng.Intn(config.SemaphoreWeightMax) + 1)

	resp, err := client.AcquireSemaphore(ctx, &grackle.AcquireSemaphoreRequest{
		NamespaceName:  lease.Namespace,
		SemaphoreName:  semName,
		LeaseId:        lease.LeaseID,
		Weight:         weight,
		TimeoutSeconds: 10,
	})
	if err != nil {
		return err
	}

	// Track if acquisition was successful
	if resp.Success {
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

	// Random number of job IDs (1 to batch size)
	numProcesses := rng.Intn(config.WaitGroupJobBatchSize) + 1
	jobIDs := make([]string, numProcesses)
	for i := range numProcesses {
		jobIDs[i] = fmt.Sprintf("load-worker-%d-%d-%d", workerID, time.Now().UnixNano(), i)
	}

	_, err := client.CompleteJobsFromWaitGroup(ctx, &grackle.CompleteJobsFromWaitGroupRequest{
		NamespaceName: ns,
		WaitGroupName: wgName,
		JobIds:        jobIDs,
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

// executeCreateLockLease creates a new lock lease
func executeCreateLockLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Generate process ID
	processID := fmt.Sprintf("load-worker-%d", workerID)

	// Calculate TTL in seconds
	ttl := config.LeaseTTL
	ttlSeconds := uint64(ttl.Seconds())

	resp, err := client.CreateLockLease(ctx, &grackle.CreateLockLeaseRequest{
		NamespaceName: ns,
		ProcessId:     processID,
		TtlSeconds:    ttlSeconds,
	})
	if err != nil {
		return err
	}

	// Track the lease
	pool.TrackLease(workerID, LeaseHandle{
		Namespace: ns,
		LeaseID:   resp.Lease.LeaseId,
		CreatedAt: time.Now(),
		TTL:       ttl,
		Type:      "lock",
	})

	return nil
}

// executeCreateSemaphoreLease creates a new semaphore lease
func executeCreateSemaphoreLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	// Generate process ID
	processID := fmt.Sprintf("load-worker-%d", workerID)

	// Calculate TTL in seconds
	ttl := config.LeaseTTL
	ttlSeconds := uint64(ttl.Seconds())

	resp, err := client.CreateSemaphoreLease(ctx, &grackle.CreateSemaphoreLeaseRequest{
		NamespaceName: ns,
		ProcessId:     processID,
		TtlSeconds:    ttlSeconds,
	})
	if err != nil {
		return err
	}

	// Track the lease
	pool.TrackLease(workerID, LeaseHandle{
		Namespace: ns,
		LeaseID:   resp.Lease.LeaseId,
		CreatedAt: time.Now(),
		TTL:       ttl,
		Type:      "semaphore",
	})

	return nil
}

// executeRefreshLockLease refreshes an existing lock lease
func executeRefreshLockLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get a random lock lease to refresh
	lease := pool.GetRandomLease(workerID, "lock")
	if lease == nil {
		// No leases to refresh, create one instead
		return executeCreateLockLease(ctx, client, pool, workerID, rng, config)
	}

	// Calculate TTL in seconds
	ttlSeconds := uint64(config.LeaseTTL.Seconds())

	_, err := client.RefreshLockLease(ctx, &grackle.RefreshLockLeaseRequest{
		NamespaceName: lease.Namespace,
		LeaseId:       lease.LeaseID,
		TtlSeconds:    ttlSeconds,
	})
	return err
}

// executeRefreshSemaphoreLease refreshes an existing semaphore lease
func executeRefreshSemaphoreLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get a random semaphore lease to refresh
	lease := pool.GetRandomLease(workerID, "semaphore")
	if lease == nil {
		// No leases to refresh, create one instead
		return executeCreateSemaphoreLease(ctx, client, pool, workerID, rng, config)
	}

	// Calculate TTL in seconds
	ttlSeconds := uint64(config.LeaseTTL.Seconds())

	_, err := client.RefreshSemaphoreLease(ctx, &grackle.RefreshSemaphoreLeaseRequest{
		NamespaceName: lease.Namespace,
		LeaseId:       lease.LeaseID,
		TtlSeconds:    ttlSeconds,
	})
	return err
}

// executeRevokeLockLease revokes an existing lock lease
func executeRevokeLockLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get a random lock lease to revoke
	lease := pool.GetAndRemoveLease(workerID, "lock")
	if lease == nil {
		// No leases to revoke, create one instead
		return executeCreateLockLease(ctx, client, pool, workerID, rng, config)
	}

	_, err := client.RevokeLockLease(ctx, &grackle.RevokeLockLeaseRequest{
		NamespaceName: lease.Namespace,
		LeaseId:       lease.LeaseID,
	})
	return err
}

// executeRevokeSemaphoreLease revokes an existing semaphore lease
func executeRevokeSemaphoreLease(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Get a random semaphore lease to revoke
	lease := pool.GetAndRemoveLease(workerID, "semaphore")
	if lease == nil {
		// No leases to revoke, create one instead
		return executeCreateSemaphoreLease(ctx, client, pool, workerID, rng, config)
	}

	_, err := client.RevokeSemaphoreLease(ctx, &grackle.RevokeSemaphoreLeaseRequest{
		NamespaceName: lease.Namespace,
		LeaseId:       lease.LeaseID,
	})
	return err
}

// executeListLocks lists locks in a namespace
func executeListLocks(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	_, err := client.ListLocks(ctx, &grackle.ListLocksRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// executeListSemaphores lists semaphores in a namespace
func executeListSemaphores(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	_, err := client.ListSemaphores(ctx, &grackle.ListSemaphoresRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// executeListWaitGroups lists wait groups in a namespace
func executeListWaitGroups(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	_, err := client.ListWaitGroups(ctx, &grackle.ListWaitGroupsRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// executeListLockLeases lists lock leases in a namespace
func executeListLockLeases(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	_, err := client.ListLockLeases(ctx, &grackle.ListLockLeasesRequest{
		NamespaceName: ns,
		Limit:         100,
	})
	return err
}

// executeListSemaphoreLeases lists semaphore leases in a namespace
func executeListSemaphoreLeases(ctx context.Context, client grackle.GrackleApi, pool *ResourcePool, workerID int, rng *rand.Rand, config *Config) error {
	// Pick random namespace
	ns := pool.namespaces[rng.Intn(len(pool.namespaces))]

	_, err := client.ListSemaphoreLeases(ctx, &grackle.ListSemaphoreLeasesRequest{
		NamespaceName: ns,
		Limit:         100,
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
