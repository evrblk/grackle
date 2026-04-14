package main

import (
	"fmt"
	"sync"
	"time"
)

// StatsCollector collects and reports statistics
type StatsCollector struct {
	mu         sync.Mutex
	startTime  time.Time

	// Overall counters
	totalRequests uint64
	totalErrors   uint64

	// Per-operation counters
	lockRequests      uint64
	lockErrors        uint64
	semaphoreRequests uint64
	semaphoreErrors   uint64
	waitgroupRequests uint64
	waitgroupErrors   uint64

	// For RPS calculation
	lastWindowRequests uint64
	lastWindowTime     time.Time
}

// NewStatsCollector creates a new statistics collector
func NewStatsCollector() *StatsCollector {
	now := time.Now()
	return &StatsCollector{
		startTime:      now,
		lastWindowTime: now,
	}
}

// RecordRequest records a request completion
func (s *StatsCollector) RecordRequest(opType OperationType, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalRequests++
	if !success {
		s.totalErrors++
	}

	// Track by operation type
	switch opType {
	case OpAcquireLock, OpReleaseLock, OpGetLock:
		s.lockRequests++
		if !success {
			s.lockErrors++
		}
	case OpAcquireSemaphore, OpReleaseSemaphore, OpGetSemaphore:
		s.semaphoreRequests++
		if !success {
			s.semaphoreErrors++
		}
	case OpAddWaitGroupJobs, OpCompleteWaitGroupJobs, OpGetWaitGroup:
		s.waitgroupRequests++
		if !success {
			s.waitgroupErrors++
		}
	}
}

// GetCurrentRPS calculates current requests per second
func (s *StatsCollector) GetCurrentRPS() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	duration := now.Sub(s.lastWindowTime).Seconds()
	if duration == 0 {
		return 0
	}

	requestsInWindow := s.totalRequests - s.lastWindowRequests
	rps := float64(requestsInWindow) / duration

	// Update window
	s.lastWindowRequests = s.totalRequests
	s.lastWindowTime = now

	return rps
}

// PrintStats prints current statistics to stdout
func (s *StatsCollector) PrintStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	elapsed := time.Since(s.startTime)
	elapsedSeconds := elapsed.Seconds()

	// Calculate success rate
	successRate := 100.0
	if s.totalRequests > 0 {
		successRate = float64(s.totalRequests-s.totalErrors) / float64(s.totalRequests) * 100
	}

	// Calculate average RPS
	avgRPS := 0.0
	if elapsedSeconds > 0 {
		avgRPS = float64(s.totalRequests) / elapsedSeconds
	}

	// Format elapsed time
	elapsedStr := formatDuration(elapsed)

	// Print main stats line
	fmt.Printf("[%s] Requests: %s | Errors: %s | RPS: %s | Success: %.1f%%\n",
		elapsedStr,
		formatNumber(s.totalRequests),
		formatNumber(s.totalErrors),
		formatNumber(uint64(avgRPS)),
		successRate,
	)

	// Print breakdown by operation type
	if s.totalRequests > 0 {
		lockPct := float64(s.lockRequests) / float64(s.totalRequests) * 100
		semaphorePct := float64(s.semaphoreRequests) / float64(s.totalRequests) * 100
		waitgroupPct := float64(s.waitgroupRequests) / float64(s.totalRequests) * 100

		fmt.Printf("  Locks: %s (%.0f%%) | Semaphores: %s (%.0f%%) | WaitGroups: %s (%.0f%%)\n",
			formatNumber(s.lockRequests), lockPct,
			formatNumber(s.semaphoreRequests), semaphorePct,
			formatNumber(s.waitgroupRequests), waitgroupPct,
		)
	}
}

// formatDuration formats a duration as HH:MM:SS
func formatDuration(d time.Duration) string {
	totalSeconds := int(d.Seconds())
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

// formatNumber formats a number with thousand separators
func formatNumber(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%d,%03d", n/1000, n%1000)
	}
	return fmt.Sprintf("%d,%03d,%03d", n/1000000, (n%1000000)/1000, n%1000)
}
