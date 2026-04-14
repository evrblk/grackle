package main

import (
	"context"
	"math/rand"
	"time"

	grackle "github.com/evrblk/evrblk-go/grackle/preview"
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
	case OpAcquireSemaphore:
		err = executeAcquireSemaphore(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpReleaseSemaphore:
		err = executeReleaseSemaphore(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpGetSemaphore:
		err = executeGetSemaphore(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpAddWaitGroupJobs:
		err = executeAddWaitGroupJobs(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpCompleteWaitGroupJobs:
		err = executeCompleteWaitGroupJobs(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
	case OpGetWaitGroup:
		err = executeGetWaitGroup(ctx, w.client, w.resourcePool, w.id, w.rng, w.config)
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
