package main

import (
	"context"
	"math/rand"
	"sync"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
	"golang.org/x/time/rate"
)

// BlockingPool runs blocking operations (acquire/wait) on background goroutines
// with a bounded number of slots, so a worker can fire a blocking call and keep
// generating load instead of parking on the server-side wait. Dispatch never
// blocks: if every slot is taken the op is dropped (and counted) rather than
// stalling the worker loop.
type BlockingPool struct {
	slots chan struct{}
	wg    sync.WaitGroup
}

// NewBlockingPool creates a pool that allows at most max concurrent blocking calls.
func NewBlockingPool(max int) *BlockingPool {
	return &BlockingPool{slots: make(chan struct{}, max)}
}

// Dispatch runs fn on a background goroutine if a slot is free, returning true.
// If the pool is at capacity it returns false immediately without running fn.
func (p *BlockingPool) Dispatch(fn func()) bool {
	select {
	case p.slots <- struct{}{}:
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.slots }()
			fn()
		}()
		return true
	default:
		return false
	}
}

// Inflight returns the number of blocking calls currently in flight.
func (p *BlockingPool) Inflight() int { return len(p.slots) }

// Wait blocks until all in-flight blocking calls have returned.
func (p *BlockingPool) Wait() { p.wg.Wait() }

// Worker represents a load generator worker
type Worker struct {
	id           int
	client       grackle.GrackleApi
	config       *Config
	resourcePool *ResourcePool
	stats        *StatsCollector
	rng          *rand.Rand
	limiter      *rate.Limiter
	blocking     *BlockingPool
}

// NewWorker creates a new worker
func NewWorker(id int, client grackle.GrackleApi, config *Config, pool *ResourcePool, stats *StatsCollector, blocking *BlockingPool) *Worker {
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
		blocking:     blocking,
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

			opType := w.selectOperation()

			if opType.isBlocking() {
				// Run blocking ops on the shared background pool so they never
				// stall this worker's load generation. If the pool is full the
				// op is dropped rather than blocking the worker. Each dispatched
				// op gets its own RNG (seeded from the worker RNG, which is only
				// touched on this goroutine) since math/rand is not safe for
				// concurrent use.
				seed := w.rng.Int63()
				dispatched := w.blocking.Dispatch(func() {
					rng := rand.New(rand.NewSource(seed))
					w.runOperation(ctx, opType, rng)
				})
				if !dispatched {
					blockingDroppedTotal.WithLabelValues(opType.String()).Inc()
				}
			} else {
				w.runOperation(ctx, opType, w.rng)
			}
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
			ttlSeconds := int64(w.config.LeaseTTL.Seconds())

			for _, lease := range w.resourcePool.GetAllLeases(w.id, "lock") {
				_, _ = w.client.RefreshLockLease(ctx, &grackle.RefreshLockLeaseRequest{
					NamespaceName: lease.Namespace,
					LeaseId:       lease.LeaseID,
					TtlSeconds:    ttlSeconds,
				})
			}

			for _, lease := range w.resourcePool.GetAllLeases(w.id, "semaphore") {
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
	r := w.rng.Intn(100)
	c := w.config

	switch {
	case r < c.LocksPct:
		return w.selectLockOp()
	case r < c.LocksPct+c.SemaphoresPct:
		return w.selectSemaphoreOp()
	case r < c.LocksPct+c.SemaphoresPct+c.WaitGroupsPct:
		return w.selectWaitGroupOp()
	default:
		return w.selectBarrierOp()
	}
}

func (w *Worker) isRead() bool { return w.rng.Intn(100) < w.config.ReadPct }

func (w *Worker) selectLockOp() OperationType {
	if w.isRead() {
		if w.rng.Intn(10) == 0 {
			return OpListLocks
		}
		return OpGetLock
	}
	if w.rng.Intn(2) == 0 {
		return OpAcquireLock
	}
	return OpReleaseLock
}

func (w *Worker) selectSemaphoreOp() OperationType {
	if w.isRead() {
		if w.rng.Intn(10) == 0 {
			return OpListSemaphores
		}
		return OpGetSemaphore
	}
	if w.rng.Intn(2) == 0 {
		return OpAcquireSemaphore
	}
	return OpReleaseSemaphore
}

func (w *Worker) selectWaitGroupOp() OperationType {
	if w.isRead() {
		switch w.rng.Intn(10) {
		case 0:
			return OpListWaitGroups
		case 1, 2:
			return OpWaitForWaitGroup // blocking observer
		default:
			return OpGetWaitGroup
		}
	}
	// Mostly complete jobs; occasionally raise the counter.
	if w.rng.Intn(10) == 0 {
		return OpUpdateWaitGroup
	}
	return OpCompleteWaitGroupJobs
}

func (w *Worker) selectBarrierOp() OperationType {
	if w.isRead() {
		switch w.rng.Intn(10) {
		case 0:
			return OpListBarriers
		case 1, 2:
			return OpWaitAtBarrier // blocking
		default:
			return OpGetBarrier
		}
	}
	// Mostly arrive; occasionally update.
	if w.rng.Intn(10) == 0 {
		return OpUpdateBarrier
	}
	return OpArriveAtBarrier
}

// runOperation executes a specific operation and records metrics. For blocking
// ops this runs on a background goroutine from the BlockingPool, with its own
// rng.
func (w *Worker) runOperation(ctx context.Context, opType OperationType, rng *rand.Rand) {
	startTime := time.Now()

	var err error
	switch opType {
	case OpAcquireLock:
		err = executeAcquireLock(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpReleaseLock:
		err = executeReleaseLock(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpGetLock:
		err = executeGetLock(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpListLocks:
		err = executeListLocks(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpCreateLockLease:
		err = executeCreateLockLease(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpAcquireSemaphore:
		err = executeAcquireSemaphore(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpReleaseSemaphore:
		err = executeReleaseSemaphore(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpGetSemaphore:
		err = executeGetSemaphore(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpListSemaphores:
		err = executeListSemaphores(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpCreateSemaphoreLease:
		err = executeCreateSemaphoreLease(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpCompleteWaitGroupJobs:
		err = executeCompleteWaitGroupJobs(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpUpdateWaitGroup:
		err = executeUpdateWaitGroup(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpWaitForWaitGroup:
		err = executeWaitForWaitGroup(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpGetWaitGroup:
		err = executeGetWaitGroup(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpListWaitGroups:
		err = executeListWaitGroups(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpArriveAtBarrier:
		err = executeArriveAtBarrier(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpWaitAtBarrier:
		err = executeWaitAtBarrier(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpUpdateBarrier:
		err = executeUpdateBarrier(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpGetBarrier:
		err = executeGetBarrier(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	case OpListBarriers:
		err = executeListBarriers(ctx, w.client, w.resourcePool, w.id, rng, w.config)
	}

	// Operations issued after the context is cancelled (e.g. during shutdown)
	// fail with a cancellation error that is not interesting load-test signal.
	if ctx.Err() != nil {
		return
	}

	duration := time.Since(startTime).Seconds()

	success := err == nil
	statusLabel := "success"
	if !success {
		statusLabel = "error"
	}

	requestsTotal.WithLabelValues(opType.String(), statusLabel).Inc()
	requestDuration.WithLabelValues(opType.String()).Observe(duration)

	if err != nil {
		errorsTotal.WithLabelValues(opType.String(), getErrorType(err)).Inc()
	}

	w.stats.RecordRequest(opType, success)
}
