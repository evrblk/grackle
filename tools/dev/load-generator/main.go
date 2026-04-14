package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	evrblk "github.com/evrblk/evrblk-go"
	grackle "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/yellowstone-common/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Parse flags
	config := parseFlags()

	// Validate configuration
	if err := config.Validate(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	log.Println("Starting Grackle Load Generator...")
	log.Printf("Configuration:")
	log.Printf("  Endpoint: %s", config.Endpoint)
	log.Printf("  Workers: %d", config.Workers)
	log.Printf("  Duration: %v", config.Duration)
	log.Printf("  Rate: %d ops/sec (0 = unlimited)", config.Rate)
	log.Printf("  Namespaces: %d", config.Namespaces)
	log.Printf("  Locks per namespace: %d", config.LocksPerNS)
	log.Printf("  Semaphores per namespace: %d", config.SemaphoresPerNS)
	log.Printf("  Wait groups per namespace: %d", config.WaitGroupsPerNS)
	log.Printf("  Operation mix: Locks=%d%%, Semaphores=%d%%, WaitGroups=%d%%",
		config.LocksPct, config.SemaphoresPct, config.WaitGroupsPct)
	log.Printf("  Read operations: %d%%", config.ReadPct)

	// Start Prometheus metrics server
	RegisterMetrics()
	metricsSrv := metrics.NewMetricsServer(config.PrometheusPort)
	metricsSrv.Start()
	defer metricsSrv.Stop()
	log.Printf("Prometheus metrics available at http://localhost:%d/metrics", config.PrometheusPort)

	// Connect to Grackle gateway
	log.Printf("Connecting to Grackle at %s...", config.Endpoint)
	conn, err := grpc.NewClient(config.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Grackle: %v", err)
	}
	defer conn.Close()

	client := grackle.NewGrackleGrpcClient(config.Endpoint, evrblk.NewNoOpSigner())
	log.Println("Connected to Grackle")

	// Setup resources
	ctx := context.Background()
	resourcePool, err := SetupResources(ctx, client, config)
	if err != nil {
		log.Fatalf("Failed to setup resources: %v", err)
	}

	// Create stats collector
	stats := NewStatsCollector()

	// Start stats logger goroutine
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	go func() {
		ticker := time.NewTicker(config.LogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-mainCtx.Done():
				return
			case <-ticker.C:
				stats.PrintStats()
				// Update RPS metric
				rps := stats.GetCurrentRPS()
				currentRPS.Set(rps)
				// Update acquired resource metrics
				acquiredLocksGauge.Set(float64(resourcePool.CountAcquiredLocks()))
				acquiredSemaphoresGauge.Set(float64(resourcePool.CountAcquiredSemaphores()))
			}
		}
	}()

	// Prepare worker context
	workerCtx := mainCtx
	var workerCancel context.CancelFunc
	if config.Duration > 0 {
		workerCtx, workerCancel = context.WithTimeout(mainCtx, config.Duration)
		defer workerCancel()
		log.Printf("Load test will run for %v", config.Duration)
	} else {
		log.Println("Load test will run indefinitely (press Ctrl+C to stop)")
	}

	// Start workers
	log.Printf("Starting %d workers...", config.Workers)
	var wg sync.WaitGroup

	for i := 0; i < config.Workers; i++ {
		wg.Add(1)
		worker := NewWorker(i, client, config, resourcePool, stats)
		go func() {
			defer wg.Done()
			worker.Run(workerCtx)
		}()
	}

	activeWorkers.Set(float64(config.Workers))
	log.Println("All workers started!")

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Println("\nReceived shutdown signal...")
	case <-workerCtx.Done():
		log.Println("\nDuration elapsed...")
	}

	// Stop workers
	log.Println("Stopping workers...")
	mainCancel()
	wg.Wait()
	activeWorkers.Set(0)
	log.Println("All workers stopped")

	// Final stats
	log.Println("\n=== Final Statistics ===")
	stats.PrintStats()

	// Cleanup resources if enabled
	if config.Cleanup {
		log.Println("\nCleaning up resources...")
		CleanupResources(context.Background(), client, resourcePool)
	} else {
		log.Println("\nSkipping cleanup (--cleanup=false)")
	}

	log.Println("Done!")
}
