package commands

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/monstera"
	"github.com/evrblk/yellowstone-common/metrics"
)

var workerCmdCfg struct {
	prometheusPort     int
	monsteraConfigPath string
}

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run Grackle background worker",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle Worker...")

		// Metrics
		metricsSrv := metrics.NewMetricsServer(workerCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Monstera cluster config
		clusterConfig, err := monstera.LoadConfigFromFile(workerCmdCfg.monsteraConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		// Create Monstera client
		monsteraClient := monstera.NewMonsteraClient(clusterConfig)
		monsteraClient.Start()
		defer monsteraClient.Stop()

		// Grackle client
		grackleCoreApiClient := grackle.NewGrackleCoreApiMonsteraStub(monsteraClient, &grackle.GrackleShardKeyCalculator{})

		// Grackle workers
		grackeLocksGarbageCollectionWorker := grackle.NewGrackleLocksGCWorker(grackleCoreApiClient)
		grackeLocksGarbageCollectionWorker.Start()
		grackeSemaphoresGarbageCollectionWorker := grackle.NewGrackleSemaphoresGCWorker(grackleCoreApiClient)
		grackeSemaphoresGarbageCollectionWorker.Start()
		grackeWaitGroupsGarbageCollectionWorker := grackle.NewGrackleWaitGroupsGCWorker(grackleCoreApiClient)
		grackeWaitGroupsGarbageCollectionWorker.Start()

		wg := sync.WaitGroup{}
		wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				log.Println("Received SIGINT. Shutting down...")
				cancel()
				metricsSrv.Stop()
				grackeLocksGarbageCollectionWorker.Stop()
				grackeSemaphoresGarbageCollectionWorker.Stop()
				grackeWaitGroupsGarbageCollectionWorker.Stop()
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

	workerCmd.PersistentFlags().IntVarP(&workerCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	workerCmd.PersistentFlags().StringVarP(&workerCmdCfg.monsteraConfigPath, "monstera-config", "", "", "Monstera cluster config path")
	err := workerCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}
}
