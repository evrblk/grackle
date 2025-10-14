package grackle

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/evrblk/grackle/pkg/corepb"
	monsterax "github.com/evrblk/monstera/x"
	"github.com/evrblk/yellowstone-common/workers"
)

var (
	grackleLocksGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_locks_gc_worker_duration_seconds",
		Help:                            "Grackle Locks Garbage Collection Worker duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleSemaphoresGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_semaphores_gc_worker_duration_seconds",
		Help:                            "Grackle Semaphores Garbage Collection Worker duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleWaitGroupsGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_wait_groups_gc_worker_duration_seconds",
		Help:                            "Grackle WaitGroups Garbage Collection Worker duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleLocksGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_locks_gc_worker_errors_total",
		Help: "Grackle Locks Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
	grackleSemaphoresGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_Semaphores_gc_worker_errors_total",
		Help: "Grackle Semaphores Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
	grackleWaitGroupsGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_wait_groups_gc_worker_errors_total",
		Help: "Grackle WaitGroups Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
)

func init() {
	prometheus.MustRegister(grackleLocksGCWorkerDuration)
	prometheus.MustRegister(grackleSemaphoresGCWorkerDuration)
	prometheus.MustRegister(grackleWaitGroupsGCWorkerDuration)
	prometheus.MustRegister(grackleLocksGCWorkerErrorsTotal)
	prometheus.MustRegister(grackleSemaphoresGCWorkerErrorsTotal)
	prometheus.MustRegister(grackleWaitGroupsGCWorkerErrorsTotal)
}

type GrackleLocksGCWorker struct {
	coreApiClient GrackleCoreApi
	worker        *workers.IntervalWorker
}

func NewGrackleLocksGCWorker(coreApiClient GrackleCoreApi) *GrackleLocksGCWorker {
	return &GrackleLocksGCWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleLocksGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleLocksGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleLocksGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleLocks")
	if err != nil {
		log.Printf("ListShards(\"GrackleLocks\"): %v", err)
		return // TODO
	}

	now := time.Now()

	done := &sync.WaitGroup{}
	done.Add(len(shards))

	for _, shard := range shards {
		go func(shardId string, now time.Time, done *sync.WaitGroup) {
			w.runGarbageCollection(shardId, now)
			done.Done()
		}(shard, now, done)
	}

	done.Wait()
}

func (w *GrackleLocksGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer monsterax.MeasureSince(grackleLocksGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunLocksGarbageCollection(context.TODO(), &corepb.RunLocksGarbageCollectionRequest{
		Now:                   now.UnixNano(),
		GcRecordsPageSize:     100,
		GcRecordLocksPageSize: 1000,
		MaxVisitedLocks:       1000,
	}, shardId)
	if err != nil {
		grackleLocksGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunLocksGarbageCollection failed: %v", err)
	}
}

type GrackleSemaphoresGCWorker struct {
	coreApiClient GrackleCoreApi
	worker        *workers.IntervalWorker
}

func NewGrackleSemaphoresGCWorker(coreApiClient GrackleCoreApi) *GrackleSemaphoresGCWorker {
	return &GrackleSemaphoresGCWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleSemaphoresGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleSemaphoresGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleSemaphoresGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleSemaphores")
	if err != nil {
		log.Printf("ListShards(\"GrackleSemaphores\"): %v", err)
		return // TODO
	}

	now := time.Now()

	done := &sync.WaitGroup{}
	done.Add(len(shards))

	for _, shard := range shards {
		go func(shardId string, now time.Time, done *sync.WaitGroup) {
			w.runGarbageCollection(shardId, now)
			done.Done()
		}(shard, now, done)
	}

	done.Wait()
}

func (w *GrackleSemaphoresGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer monsterax.MeasureSince(grackleSemaphoresGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunSemaphoresGarbageCollection(context.TODO(), &corepb.RunSemaphoresGarbageCollectionRequest{
		Now:                        now.UnixNano(),
		GcRecordsPageSize:          100,
		GcRecordSemaphoresPageSize: 1000,
		MaxVisitedSemaphores:       1000,
	}, shardId)
	if err != nil {
		grackleSemaphoresGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunSemaphoresGarbageCollection failed: %v", err)
	}
}

type GrackleWaitGroupsGCWorker struct {
	coreApiClient GrackleCoreApi
	worker        *workers.IntervalWorker
}

func NewGrackleWaitGroupsGCWorker(coreApiClient GrackleCoreApi) *GrackleWaitGroupsGCWorker {
	return &GrackleWaitGroupsGCWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *GrackleWaitGroupsGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *GrackleWaitGroupsGCWorker) Stop() {
	w.worker.Stop()
}

func (w *GrackleWaitGroupsGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("GrackleWaitGroups")
	if err != nil {
		log.Printf("ListShards(\"GrackleWaitGroups\"): %v", err)
		return // TODO
	}

	now := time.Now()

	done := &sync.WaitGroup{}
	done.Add(len(shards))

	for _, shard := range shards {
		go func(shardId string, now time.Time, done *sync.WaitGroup) {
			w.runGarbageCollection(shardId, now)
			done.Done()
		}(shard, now, done)
	}

	done.Wait()
}

func (w *GrackleWaitGroupsGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer monsterax.MeasureSince(grackleWaitGroupsGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunWaitGroupsGarbageCollection(context.TODO(), &corepb.RunWaitGroupsGarbageCollectionRequest{
		Now: now.UnixNano(),
	}, shardId)
	if err != nil {
		grackleWaitGroupsGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunWaitGroupsGarbageCollection failed: %v", err)
	}
}
