package main

import (
	"context"
	"math/rand"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
	"golang.org/x/time/rate"
)

// Worker represents a load generator worker
type Worker struct {
	id           int
	client       grackle.GrackleApi
	config       *Config
	resourcePool *ResourcePool
	stats        *StatsCollector
	rng          *rand.Rand
	limiter      *rate.Limiter
}

// NewWorker creates a new worker
func NewWorker(id int, client grackle.GrackleApi, config *Config, pool *ResourcePool, stats *StatsCollector) *Worker {
	// Create per-worker RNG for thread safety
	source := rand.NewSource(time.Now().UnixNano() + int64(id))
	rng := rand.New(source)

	// Create rate limiter if rate is specified
	var limiter *rate.Limiter
	if config.Rate > 0 {
		perWorkerRate := float64(config.Rate) / float64(config.Workers)
		limiter = rate.NewLimiter(rate.Limit(perWorkerRate), 1)
	}

	return &Worker{
		id:           id,
		client:       client,
		config:       config,
		resourcePool: pool,
		stats:        stats,
		rng:          rng,
		limiter:      limiter,
	}
}

// Run executes the worker loop
func (w *Worker) Run(ctx context.Context) {
	// Start lease refresh background task
	go w.runLeaseRefresh(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Rate limiting
			if w.limiter != nil {
				if err := w.limiter.Wait(ctx); err != nil {
					return
				}
			}

			// Select and execute operation
			opType := w.selectOperation()
			w.executeOperation(ctx, opType)
		}
	}
}

// runLeaseRefresh periodically refreshes all leases for this worker
func (w *Worker) runLeaseRefresh(ctx context.Context) {
	ticker := time.NewTicker(w.config.LeaseRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ttlSeconds := uint64(w.config.LeaseTTL.Seconds())

			// Refresh all lock leases
			lockLeases := w.resourcePool.GetAllLeases(w.id, "lock")
			for _, lease := range lockLeases {
				_, _ = w.client.RefreshLockLease(ctx, &grackle.RefreshLockLeaseRequest{
					NamespaceName: lease.Namespace,
					LeaseId:       lease.LeaseID,
					TtlSeconds:    ttlSeconds,
				})
			}

			// Refresh all semaphore leases
			semLeases := w.resourcePool.GetAllLeases(w.id, "semaphore")
			for _, lease := range semLeases {
				_, _ = w.client.RefreshSemaphoreLease(ctx, &grackle.RefreshSemaphoreLeaseRequest{
					NamespaceName: lease.Namespace,
					LeaseId:       lease.LeaseID,
					TtlSeconds:    ttlSeconds,
				})
			}
		}
	}
}

// selectOperation selects an operation type based on configured percentages
func (w *Worker) selectOperation() OperationType {
	// Generate random number 0-99
	r := w.rng.Intn(100)

	// Determine operation category (locks, semaphores, waitgroups)
	var category string
	if r < w.config.LocksPct {
		category = "locks"
	} else if r < w.config.LocksPct+w.config.SemaphoresPct {
		category = "semaphores"
	} else {
		category = "waitgroups"
	}

	// Within each category, choose specific operation based on read-pct
	readOp := w.rng.Intn(100) < w.config.ReadPct

	switch category {
	case "locks":
		if readOp {
			return OpGetLock
		}
		// For writes, randomly choose between acquire and release
		if w.rng.Intn(2) == 0 {
			return OpAcquireLock
		}
		return OpReleaseLock

	case "semaphores":
		if readOp {
			return OpGetSemaphore
		}
		// For writes, randomly choose between acquire and release
		if w.rng.Intn(2) == 0 {
			return OpAcquireSemaphore
		}
		return OpReleaseSemaphore

	case "waitgroups":
		if readOp {
			return OpGetWaitGroup
		}
		// For writes, randomly choose between add and complete
		if w.rng.Intn(2) == 0 {
			return OpAddWaitGroupJobs
		}
		return OpCompleteWaitGroupJobs

	default:
		return OpGetLock
	}
}

// executeOperation executes a specific operation and records metrics
func (w *Worker) executeOperation(ctx context.Context, opType OperationType) {
	startTime := time.Now()

	var err error
	switch opType {
	case OpAcquireLock:
		err = executeAcquireLock(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpReleaseLock:
		err = executeReleaseLock(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpGetLock:
		err = executeGetLock(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpListLocks:
		err = executeListLocks(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpCreateLockLease:
		err = executeCreateLockLease(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpRefreshLockLease:
		err = executeRefreshLockLease(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpRevokeLockLease:
		err = executeRevokeLockLease(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpListLockLeases:
		err = executeListLockLeases(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpAcquireSemaphore:
		err = executeAcquireSemaphore(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpReleaseSemaphore:
		err = executeReleaseSemaphore(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpGetSemaphore:
		err = executeGetSemaphore(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpListSemaphores:
		err = executeListSemaphores(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpCreateSemaphoreLease:
		err = executeCreateSemaphoreLease(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpRefreshSemaphoreLease:
		err = executeRefreshSemaphoreLease(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpRevokeSemaphoreLease:
		err = executeRevokeSemaphoreLease(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpListSemaphoreLeases:
		err = executeListSemaphoreLeases(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpAddWaitGroupJobs:
		err = executeAddWaitGroupJobs(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpCompleteWaitGroupJobs:
		err = executeCompleteWaitGroupJobs(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpGetWaitGroup:
		err = executeGetWaitGroup(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpListWaitGroups:
		err = executeListWaitGroups(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	}

	duration := time.Since(startTime).Seconds()

	// Record metrics
	success := err == nil
	status := "success"
	if !success {
		status = "error"
	}

	requestsTotal.WithLabelValues(opType.String(), status).Inc()
	requestDuration.WithLabelValues(opType.String()).Observe(duration)

	if err != nil {
		errType := getErrorType(err)
		errorsTotal.WithLabelValues(opType.String(), errType).Inc()
	}

	// Record in stats
	w.stats.RecordRequest(opType, success)
}
