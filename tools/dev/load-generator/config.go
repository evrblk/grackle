package main

import (
	"flag"
	"fmt"
	"time"
)

type Config struct {
	// Connection
	Endpoint string

	// Load parameters
	Workers  int
	Duration time.Duration
	Rate     int

	// MaxInflightBlocking bounds how many blocking calls (AcquireLock,
	// AcquireSemaphore, WaitForWaitGroup, WaitAtBarrier) may be in flight at
	// once across all workers. Blocking calls run on background goroutines so
	// they never stall a worker's load-generation loop; this cap keeps the
	// number of parked goroutines (and parked server-side requests) bounded.
	MaxInflightBlocking int

	// Resource configuration
	Namespaces      int
	LocksPerNS      int
	SemaphoresPerNS int
	WaitGroupsPerNS int
	BarriersPerNS   int

	// Operation mix (percentages, must sum to 100)
	LocksPct      int
	SemaphoresPct int
	WaitGroupsPct int
	BarriersPct   int
	ReadPct       int

	// Lock-specific
	ExclusiveLockPct int

	// Semaphore-specific
	SemaphorePermits   int
	SemaphoreWeightMax int

	// Wait group-specific
	WaitGroupInitialCounter int
	WaitGroupJobBatchSize   int
	// WaitGroupExpiresIn is how far in the future a wait group's expires_at is
	// set, both at creation and when its counter is raised.
	WaitGroupExpiresIn time.Duration
	// WaitGroupDeleteAfterFinished controls how long a finished wait group is
	// retained before GC. The generator proactively recreates finished groups,
	// so a short retention keeps names reusable.
	WaitGroupDeleteAfterFinished time.Duration

	// Barrier-specific
	BarrierExpectedProcesses   int
	BarrierDeleteInactiveAfter time.Duration

	// Blocking-call timeouts. These map to the server-side timeout_seconds on
	// the blocking APIs. The client RPC deadline is set comfortably above them.
	AcquireTimeout time.Duration // AcquireLock / AcquireSemaphore
	WaitTimeout    time.Duration // WaitForWaitGroup / WaitAtBarrier

	// Lease-specific
	LeaseTTL             time.Duration
	LeaseRefreshInterval time.Duration

	// General
	PrometheusPort int
	LogInterval    time.Duration
	Cleanup        bool
}

func parseFlags() *Config {
	config := &Config{}

	// Connection
	flag.StringVar(&config.Endpoint, "endpoint", "localhost:9000", "Grackle gateway endpoint")

	// Load parameters
	flag.IntVar(&config.Workers, "workers", 10, "Number of concurrent workers")
	flag.DurationVar(&config.Duration, "duration", 60*time.Second, "Load test duration (0 = infinite)")
	flag.IntVar(&config.Rate, "rate", 0, "Target operations per second (0 = unlimited)")
	flag.IntVar(&config.MaxInflightBlocking, "max-inflight-blocking", 1000, "Max concurrent blocking calls (acquire/wait) in flight across all workers")

	// Resource configuration
	flag.IntVar(&config.Namespaces, "namespaces", 5, "Number of namespaces to create")
	flag.IntVar(&config.LocksPerNS, "locks-per-ns", 100, "Locks per namespace")
	flag.IntVar(&config.SemaphoresPerNS, "semaphores-per-ns", 20, "Semaphores per namespace")
	flag.IntVar(&config.WaitGroupsPerNS, "waitgroups-per-ns", 10, "Wait groups per namespace")
	flag.IntVar(&config.BarriersPerNS, "barriers-per-ns", 10, "Barriers per namespace")

	// Operation mix
	flag.IntVar(&config.LocksPct, "locks-pct", 30, "Percentage of lock operations")
	flag.IntVar(&config.SemaphoresPct, "semaphores-pct", 25, "Percentage of semaphore operations")
	flag.IntVar(&config.WaitGroupsPct, "waitgroups-pct", 25, "Percentage of wait group operations")
	flag.IntVar(&config.BarriersPct, "barriers-pct", 20, "Percentage of barrier operations")
	flag.IntVar(&config.ReadPct, "read-pct", 30, "Percentage of read operations (for each primitive type)")

	// Lock-specific
	flag.IntVar(&config.ExclusiveLockPct, "exclusive-lock-pct", 50, "Percentage of exclusive locks")

	// Semaphore-specific
	flag.IntVar(&config.SemaphorePermits, "semaphore-permits", 100, "Initial permits per semaphore")
	flag.IntVar(&config.SemaphoreWeightMax, "semaphore-weight-max", 10, "Max weight for semaphore acquire")

	// Wait group-specific
	flag.IntVar(&config.WaitGroupInitialCounter, "waitgroup-initial-counter", 100, "Initial counter for wait groups")
	flag.IntVar(&config.WaitGroupJobBatchSize, "waitgroup-job-batch-size", 5, "Jobs to complete at once")
	flag.DurationVar(&config.WaitGroupExpiresIn, "waitgroup-expires-in", 1*time.Hour, "How far in the future a wait group's expires_at is set")
	flag.DurationVar(&config.WaitGroupDeleteAfterFinished, "waitgroup-delete-after-finished", 10*time.Second, "Retention of a finished wait group before GC")

	// Barrier-specific
	flag.IntVar(&config.BarrierExpectedProcesses, "barrier-expected-processes", 4, "Expected participants per barrier")
	flag.DurationVar(&config.BarrierDeleteInactiveAfter, "barrier-delete-inactive-after", 1*time.Hour, "Auto-delete a barrier after this much inactivity")

	// Blocking timeouts
	flag.DurationVar(&config.AcquireTimeout, "acquire-timeout", 2*time.Second, "Server-side timeout for AcquireLock/AcquireSemaphore")
	flag.DurationVar(&config.WaitTimeout, "wait-timeout", 5*time.Second, "Server-side timeout for WaitForWaitGroup/WaitAtBarrier")

	// Lease-specific
	flag.DurationVar(&config.LeaseTTL, "lease-ttl", 30*time.Second, "Lease time-to-live")
	flag.DurationVar(&config.LeaseRefreshInterval, "lease-refresh-interval", 10*time.Second, "Lease refresh interval")

	// General
	flag.IntVar(&config.PrometheusPort, "prometheus-port", 2113, "Prometheus metrics port")
	flag.DurationVar(&config.LogInterval, "log-interval", 5*time.Second, "Stats logging interval")
	flag.BoolVar(&config.Cleanup, "cleanup", true, "Cleanup resources on shutdown")

	flag.Parse()

	return config
}

// timeoutSeconds converts a duration to a positive whole number of seconds, as
// expected by the server-side timeout_seconds fields (rounding up, min 1).
func timeoutSeconds(d time.Duration) int32 {
	secs := int32(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	if secs < 1 {
		secs = 1
	}
	return secs
}

// AcquireTimeoutSeconds is the timeout_seconds value sent on acquire calls.
func (c *Config) AcquireTimeoutSeconds() int32 { return timeoutSeconds(c.AcquireTimeout) }

// WaitTimeoutSeconds is the timeout_seconds value sent on wait calls.
func (c *Config) WaitTimeoutSeconds() int32 { return timeoutSeconds(c.WaitTimeout) }

// acquireCtxTimeout is the client RPC deadline for acquire calls, kept
// comfortably above the server-side timeout.
func (c *Config) acquireCtxTimeout() time.Duration { return c.AcquireTimeout + 5*time.Second }

// waitCtxTimeout is the client RPC deadline for wait calls, kept comfortably
// above the server-side timeout.
func (c *Config) waitCtxTimeout() time.Duration { return c.WaitTimeout + 5*time.Second }

func (c *Config) Validate() error {
	// Check operation percentages sum to 100
	total := c.LocksPct + c.SemaphoresPct + c.WaitGroupsPct + c.BarriersPct
	if total != 100 {
		return fmt.Errorf("operation percentages must sum to 100, got %d", total)
	}

	// Check individual percentages are valid
	for _, p := range []struct {
		name string
		val  int
	}{
		{"locks-pct", c.LocksPct},
		{"semaphores-pct", c.SemaphoresPct},
		{"waitgroups-pct", c.WaitGroupsPct},
		{"barriers-pct", c.BarriersPct},
		{"read-pct", c.ReadPct},
		{"exclusive-lock-pct", c.ExclusiveLockPct},
	} {
		if p.val < 0 || p.val > 100 {
			return fmt.Errorf("%s must be between 0 and 100, got %d", p.name, p.val)
		}
	}

	// Check positive values
	if c.Workers <= 0 {
		return fmt.Errorf("workers must be positive, got %d", c.Workers)
	}
	if c.MaxInflightBlocking <= 0 {
		return fmt.Errorf("max-inflight-blocking must be positive, got %d", c.MaxInflightBlocking)
	}
	if c.Duration < 0 {
		return fmt.Errorf("duration cannot be negative, got %v", c.Duration)
	}
	if c.Rate < 0 {
		return fmt.Errorf("rate cannot be negative, got %d", c.Rate)
	}
	if c.Namespaces <= 0 {
		return fmt.Errorf("namespaces must be positive, got %d", c.Namespaces)
	}
	if c.LocksPerNS < 0 {
		return fmt.Errorf("locks-per-ns cannot be negative, got %d", c.LocksPerNS)
	}
	if c.SemaphoresPerNS < 0 {
		return fmt.Errorf("semaphores-per-ns cannot be negative, got %d", c.SemaphoresPerNS)
	}
	if c.WaitGroupsPerNS < 0 {
		return fmt.Errorf("waitgroups-per-ns cannot be negative, got %d", c.WaitGroupsPerNS)
	}
	if c.BarriersPerNS < 0 {
		return fmt.Errorf("barriers-per-ns cannot be negative, got %d", c.BarriersPerNS)
	}
	if c.SemaphorePermits <= 0 {
		return fmt.Errorf("semaphore-permits must be positive, got %d", c.SemaphorePermits)
	}
	if c.SemaphoreWeightMax <= 0 {
		return fmt.Errorf("semaphore-weight-max must be positive, got %d", c.SemaphoreWeightMax)
	}
	if c.SemaphoreWeightMax > c.SemaphorePermits {
		return fmt.Errorf("semaphore-weight-max (%d) cannot exceed semaphore-permits (%d)", c.SemaphoreWeightMax, c.SemaphorePermits)
	}
	if c.WaitGroupInitialCounter <= 0 {
		return fmt.Errorf("waitgroup-initial-counter must be positive, got %d", c.WaitGroupInitialCounter)
	}
	if c.WaitGroupJobBatchSize <= 0 {
		return fmt.Errorf("waitgroup-job-batch-size must be positive, got %d", c.WaitGroupJobBatchSize)
	}
	if c.WaitGroupExpiresIn <= 0 {
		return fmt.Errorf("waitgroup-expires-in must be positive, got %v", c.WaitGroupExpiresIn)
	}
	if c.WaitGroupDeleteAfterFinished < 0 {
		return fmt.Errorf("waitgroup-delete-after-finished cannot be negative, got %v", c.WaitGroupDeleteAfterFinished)
	}
	if c.BarrierExpectedProcesses <= 0 {
		return fmt.Errorf("barrier-expected-processes must be positive, got %d", c.BarrierExpectedProcesses)
	}
	if c.BarrierDeleteInactiveAfter <= 0 {
		return fmt.Errorf("barrier-delete-inactive-after must be positive, got %v", c.BarrierDeleteInactiveAfter)
	}
	if c.AcquireTimeout <= 0 {
		return fmt.Errorf("acquire-timeout must be positive, got %v", c.AcquireTimeout)
	}
	if c.WaitTimeout <= 0 {
		return fmt.Errorf("wait-timeout must be positive, got %v", c.WaitTimeout)
	}
	if c.LeaseTTL <= 0 {
		return fmt.Errorf("lease-ttl must be positive, got %v", c.LeaseTTL)
	}
	if c.LeaseRefreshInterval <= 0 {
		return fmt.Errorf("lease-refresh-interval must be positive, got %v", c.LeaseRefreshInterval)
	}
	if c.LeaseRefreshInterval >= c.LeaseTTL {
		return fmt.Errorf("lease-refresh-interval must be less than lease-ttl, got refresh=%v ttl=%v", c.LeaseRefreshInterval, c.LeaseTTL)
	}

	// Check that at least one operation type is enabled
	if c.LocksPct == 0 && c.SemaphoresPct == 0 && c.WaitGroupsPct == 0 && c.BarriersPct == 0 {
		return fmt.Errorf("at least one operation type must be enabled")
	}

	// Check that resources exist for enabled operation types
	if c.LocksPct > 0 && c.LocksPerNS == 0 {
		return fmt.Errorf("locks operations enabled but locks-per-ns is 0")
	}
	if c.SemaphoresPct > 0 && c.SemaphoresPerNS == 0 {
		return fmt.Errorf("semaphore operations enabled but semaphores-per-ns is 0")
	}
	if c.WaitGroupsPct > 0 && c.WaitGroupsPerNS == 0 {
		return fmt.Errorf("wait group operations enabled but waitgroups-per-ns is 0")
	}
	if c.BarriersPct > 0 && c.BarriersPerNS == 0 {
		return fmt.Errorf("barrier operations enabled but barriers-per-ns is 0")
	}

	return nil
}
