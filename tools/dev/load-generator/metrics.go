package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// requestsTotal tracks total number of requests by operation and status
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "load_generator_requests_total",
			Help: "Total number of requests by operation type and status",
		},
		[]string{"operation", "status"},
	)

	// requestDuration tracks request latency by operation
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:                            "load_generator_request_duration_seconds",
			Help:                            "Request duration by operation type",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 0,
		},
		[]string{"operation"},
	)

	// errorsTotal tracks total number of errors by operation and error type
	errorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "load_generator_errors_total",
			Help: "Total number of errors by operation type and error type",
		},
		[]string{"operation", "error_type"},
	)

	// activeWorkers tracks number of active worker goroutines
	activeWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_active_workers",
			Help: "Number of active worker goroutines",
		},
	)

	// acquiredLocksGauge tracks number of currently acquired locks
	acquiredLocksGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_acquired_locks",
			Help: "Number of currently acquired locks",
		},
	)

	// acquiredSemaphoresGauge tracks number of currently acquired semaphores
	acquiredSemaphoresGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_acquired_semaphores",
			Help: "Number of currently acquired semaphores",
		},
	)

	// currentRPS tracks current requests per second (sliding window)
	currentRPS = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_requests_per_second",
			Help: "Current requests per second (sliding window)",
		},
	)
)

// RegisterMetrics registers all metrics with Prometheus
func RegisterMetrics() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(errorsTotal)
	prometheus.MustRegister(activeWorkers)
	prometheus.MustRegister(acquiredLocksGauge)
	prometheus.MustRegister(acquiredSemaphoresGauge)
	prometheus.MustRegister(currentRPS)
}
