package workers

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	grackleLocksGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_locks_gc_worker_duration_seconds",
		Help:                            "Grackle Locks Garbage Collection Worker duration",
		Buckets:                         []float64{},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleSemaphoresGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_semaphores_gc_worker_duration_seconds",
		Help:                            "Grackle Semaphores Garbage Collection Worker duration",
		Buckets:                         []float64{},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleWaitGroupsGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_wait_groups_gc_worker_duration_seconds",
		Help:                            "Grackle WaitGroups Garbage Collection Worker duration",
		Buckets:                         []float64{},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleBarriersGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_barriers_gc_worker_duration_seconds",
		Help:                            "Grackle Barriers Garbage Collection Worker duration",
		Buckets:                         []float64{},
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	grackleLocksGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_locks_gc_worker_errors_total",
		Help: "Grackle Locks Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
	grackleSemaphoresGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_semaphores_gc_worker_errors_total",
		Help: "Grackle Semaphores Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
	grackleWaitGroupsGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_wait_groups_gc_worker_errors_total",
		Help: "Grackle WaitGroups Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
	grackleBarriersGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_barriers_gc_worker_errors_total",
		Help: "Grackle Barriers Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
)

// RegisterMetrics registers the worker metrics with the given registerer.
// Call once at startup, e.g. RegisterMetrics(prometheus.DefaultRegisterer).
// It panics if a metric is already registered.
func RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(grackleLocksGCWorkerDuration)
	registerer.MustRegister(grackleSemaphoresGCWorkerDuration)
	registerer.MustRegister(grackleWaitGroupsGCWorkerDuration)
	registerer.MustRegister(grackleBarriersGCWorkerDuration)
	registerer.MustRegister(grackleLocksGCWorkerErrorsTotal)
	registerer.MustRegister(grackleSemaphoresGCWorkerErrorsTotal)
	registerer.MustRegister(grackleWaitGroupsGCWorkerErrorsTotal)
	registerer.MustRegister(grackleBarriersGCWorkerErrorsTotal)
}
