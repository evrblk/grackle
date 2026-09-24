package commands

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/evrblk/monstera"
	monstera_grpc "github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/yellowstone-common/metrics"

	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/workers"
)

var workerCmdCfg struct {
	prometheusListenAddr string
	nodes                monsteraNodesFlags
	log                  logFlags
}

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run Grackle background worker",
	Run: func(cmd *cobra.Command, args []string) {
		baseLogger := setupLogger(workerCmdCfg.log).With("service_name", "worker")
		baseLogger.Info("Initializing Grackle Worker...")

		// Metrics
		workers.RegisterMetrics(prometheus.DefaultRegisterer)
		metricsSrv := metrics.NewMetricsServer(workerCmdCfg.prometheusListenAddr)
		metricsSrv.Start()

		// Node discovery + polling config provider.
		discovery, err := buildNodeDiscovery(workerCmdCfg.nodes)
		if err != nil {
			baseLogger.Error(err.Error())
			os.Exit(1)
		}
		adminClient := monstera_grpc.NewAdminClient()
		provider := monstera.NewPollingClusterConfigProvider(discovery, adminClient, monstera.PollingOptions{})

		// Data plane + Monstera client
		transport := monstera_grpc.NewDataPlaneClient()
		monsteraClient := monstera.NewMonsteraClient(provider, transport, monstera.DefaultClientConfig())

		ctx, cancel := context.WithCancel(context.Background())
		if err := monsteraClient.Start(ctx); err != nil {
			baseLogger.Error("failed to start monstera client", "error", err)
			os.Exit(1)
		}
		defer monsteraClient.Stop()
		defer adminClient.Close()

		// Grackle client
		grackleCoreApiClient := coreapis.NewGrackleMonsteraStub(monsteraClient)

		// Grackle workers
		grackeLocksGarbageCollectionWorker := workers.NewGrackleLocksGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-locks-gc-worker"))
		grackeLocksGarbageCollectionWorker.Start()
		grackeSemaphoresGarbageCollectionWorker := workers.NewGrackleSemaphoresGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-semaphores-gc-worker"))
		grackeSemaphoresGarbageCollectionWorker.Start()
		grackeWaitGroupsGarbageCollectionWorker := workers.NewGrackleWaitGroupsGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-wait-groups-gc-worker"))
		grackeWaitGroupsGarbageCollectionWorker.Start()
		grackeBarriersGarbageCollectionWorker := workers.NewGrackleBarriersGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-barriers-gc-worker"))
		grackeBarriersGarbageCollectionWorker.Start()

		wg := sync.WaitGroup{}
		wg.Add(1)
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				baseLogger.Info("Received SIGINT. Shutting down...")
				cancel()
				metricsSrv.Stop()
				grackeLocksGarbageCollectionWorker.Stop()
				grackeSemaphoresGarbageCollectionWorker.Stop()
				grackeWaitGroupsGarbageCollectionWorker.Stop()
				grackeBarriersGarbageCollectionWorker.Stop()
			case <-ctx.Done():
			}
			wg.Done()
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		wg.Wait()
	},
}

func init() {
	runCmd.AddCommand(workerCmd)

	workerCmd.PersistentFlags().StringVarP(&workerCmdCfg.prometheusListenAddr, "prometheus-listen-addr", "", ":2112", "Prometheus metrics bind address")

	addMonsteraNodesFlags(workerCmd, &workerCmdCfg.nodes)
	addLogFlags(workerCmd, &workerCmdCfg.log)
}
