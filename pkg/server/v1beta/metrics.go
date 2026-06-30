package v1beta

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
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
	prometheus.MustRegister(totalRequestsCounter)
	prometheus.MustRegister(failedRequestsCounter)
	prometheus.MustRegister(requestsDuration)
}
