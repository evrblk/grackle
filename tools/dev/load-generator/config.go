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

	// Resource configuration
	Namespaces      int
	LocksPerNS      int
	SemaphoresPerNS int
	WaitGroupsPerNS int

	// Operation mix (percentages)
	LocksPct      int
	SemaphoresPct int
	WaitGroupsPct int
	ReadPct       int

	// Lock-specific
	ExclusiveLockPct int

	// Semaphore-specific
	SemaphorePermits   int
	SemaphoreWeightMax int

	// Wait group-specific
	WaitGroupInitialCounter int
	WaitGroupJobBatchSize   int

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

	// Resource configuration
	flag.IntVar(&config.Namespaces, "namespaces", 5, "Number of namespaces to create")
	flag.IntVar(&config.LocksPerNS, "locks-per-ns", 100, "Locks per namespace")
	flag.IntVar(&config.SemaphoresPerNS, "semaphores-per-ns", 20, "Semaphores per namespace")
	flag.IntVar(&config.WaitGroupsPerNS, "waitgroups-per-ns", 10, "Wait groups per namespace")

	// Operation mix
	flag.IntVar(&config.LocksPct, "locks-pct", 40, "Percentage of lock operations")
	flag.IntVar(&config.SemaphoresPct, "semaphores-pct", 30, "Percentage of semaphore operations")
	flag.IntVar(&config.WaitGroupsPct, "waitgroups-pct", 30, "Percentage of wait group operations")
	flag.IntVar(&config.ReadPct, "read-pct", 30, "Percentage of read operations (for each primitive type)")

	// Lock-specific
	flag.IntVar(&config.ExclusiveLockPct, "exclusive-lock-pct", 50, "Percentage of exclusive locks")

	// Semaphore-specific
	flag.IntVar(&config.SemaphorePermits, "semaphore-permits", 100, "Initial permits per semaphore")
	flag.IntVar(&config.SemaphoreWeightMax, "semaphore-weight-max", 10, "Max weight for semaphore acquire")

	// Wait group-specific
	flag.IntVar(&config.WaitGroupInitialCounter, "waitgroup-initial-counter", 100, "Initial counter for wait groups")
	flag.IntVar(&config.WaitGroupJobBatchSize, "waitgroup-job-batch-size", 5, "Jobs to add/complete at once")

	// General
	flag.IntVar(&config.PrometheusPort, "prometheus-port", 2113, "Prometheus metrics port")
	flag.DurationVar(&config.LogInterval, "log-interval", 5*time.Second, "Stats logging interval")
	flag.BoolVar(&config.Cleanup, "cleanup", true, "Cleanup resources on shutdown")

	flag.Parse()

	return config
}

func (c *Config) Validate() error {
	// Check operation percentages sum to 100
	total := c.LocksPct + c.SemaphoresPct + c.WaitGroupsPct
	if total != 100 {
		return fmt.Errorf("operation percentages must sum to 100, got %d", total)
	}

	// Check individual percentages are valid
	if c.LocksPct < 0 || c.LocksPct > 100 {
		return fmt.Errorf("locks-pct must be between 0 and 100, got %d", c.LocksPct)
	}
	if c.SemaphoresPct < 0 || c.SemaphoresPct > 100 {
		return fmt.Errorf("semaphores-pct must be between 0 and 100, got %d", c.SemaphoresPct)
	}
	if c.WaitGroupsPct < 0 || c.WaitGroupsPct > 100 {
		return fmt.Errorf("waitgroups-pct must be between 0 and 100, got %d", c.WaitGroupsPct)
	}
	if c.ReadPct < 0 || c.ReadPct > 100 {
		return fmt.Errorf("read-pct must be between 0 and 100, got %d", c.ReadPct)
	}
	if c.ExclusiveLockPct < 0 || c.ExclusiveLockPct > 100 {
		return fmt.Errorf("exclusive-lock-pct must be between 0 and 100, got %d", c.ExclusiveLockPct)
	}

	// Check positive values
	if c.Workers <= 0 {
		return fmt.Errorf("workers must be positive, got %d", c.Workers)
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
	if c.SemaphorePermits <= 0 {
		return fmt.Errorf("semaphore-permits must be positive, got %d", c.SemaphorePermits)
	}
	if c.SemaphoreWeightMax <= 0 {
		return fmt.Errorf("semaphore-weight-max must be positive, got %d", c.SemaphoreWeightMax)
	}
	if c.WaitGroupInitialCounter <= 0 {
		return fmt.Errorf("waitgroup-initial-counter must be positive, got %d", c.WaitGroupInitialCounter)
	}
	if c.WaitGroupJobBatchSize <= 0 {
		return fmt.Errorf("waitgroup-job-batch-size must be positive, got %d", c.WaitGroupJobBatchSize)
	}

	// Check that at least one operation type is enabled
	if c.LocksPct == 0 && c.SemaphoresPct == 0 && c.WaitGroupsPct == 0 {
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

	return nil
}
