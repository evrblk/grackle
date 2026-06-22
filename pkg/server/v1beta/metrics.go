package v1beta

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	locksOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_locks_operations_total",
		Help: "Locks operations total",
	})
	semaphoresOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_semaphores_operations_total",
		Help: "Semaphores operations total",
	})
	waitGroupsOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_wait_groups_operations_total",
		Help: "Wait Groups operations total",
	})
	barriersOperationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "grackle_barriers_operations_total",
		Help: "Barriers operations total",
	})

	totalRequestsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_server_requests_total",
		Help: "Total number of requests",
	}, []string{"method"})
	failedRequestsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grackle_server_requests_failed",
		Help: "Number of failed requests",
	}, []string{"method", "error"})
	requestsDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "grackle_server_request_duration_seconds",
		Help:                            "Request duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"method"})
)

func RegisterMetrics() {
	prometheus.MustRegister(locksOperationsTotal)
	prometheus.MustRegister(semaphoresOperationsTotal)
	prometheus.MustRegister(waitGroupsOperationsTotal)
	prometheus.MustRegister(barriersOperationsTotal)
}
