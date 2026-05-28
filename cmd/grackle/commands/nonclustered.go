package commands

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/monsteragen"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	grackle_preview "github.com/evrblk/grackle/pkg/server/preview"
	"github.com/evrblk/grackle/pkg/sharding"
	"github.com/evrblk/grackle/pkg/waitgroups"
	"github.com/evrblk/grackle/pkg/workers"
)

var nonclusteredCmdCfg struct {
	port           int
	prometheusPort int
	authKeysPath   string
	shardsCount    int
	dataDir        string
}

var nonclusteredCmd = &cobra.Command{
	Use:   "nonclustered",
	Short: "Run Grackle in all-in-one nonclustered mode",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle...")

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nonclusteredCmdCfg.port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Metrics
		grackle_preview.RegisterMetrics()
		metricsSrv := metrics.NewMetricsServer(nonclusteredCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(filepath.Join(nonclusteredCmdCfg.dataDir, "data"))
		if err != nil {
			log.Fatalf("failed to create data store: %v", err)
		}

		// Middleware
		unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
		if nonclusteredCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, grackle_preview.NewAuthenticationMiddleware(nonclusteredCmdCfg.authKeysPath).Unary)
		}

		// Grackle nonclustered client
		coresFactory := &monsteragen.GrackleNonclusteredApplicationCoresFactory{
			GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleLocksCoreApi {
				return locks.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
			GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleNamespacesCoreApi {
				return namespaces.NewCore(dataStore, lowerBound, upperBound)
			},
			GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleSemaphoresCoreApi {
				return semaphores.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
			GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleWaitGroupsCoreApi {
				return waitgroups.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
			GrackleBarriersCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) monsteragen.GrackleBarriersCoreApi {
				return barriers.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
		}
		grackleCoreApiClient := monsteragen.NewGrackleCoreApiNonclusteredStub(nonclusteredCmdCfg.shardsCount, coresFactory, &sharding.GrackleShardKeyCalculator{})

		// Grackle workers
		grackeLocksGarbageCollectionWorker := workers.NewGrackleLocksGCWorker(grackleCoreApiClient)
		grackeLocksGarbageCollectionWorker.Start()
		grackeSemaphoresGarbageCollectionWorker := workers.NewGrackleSemaphoresGCWorker(grackleCoreApiClient)
		grackeSemaphoresGarbageCollectionWorker.Start()
		grackeWaitGroupsGarbageCollectionWorker := workers.NewGrackleWaitGroupsGCWorker(grackleCoreApiClient)
		grackeWaitGroupsGarbageCollectionWorker.Start()

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
		)

		ctx, cancel := context.WithCancel(context.Background())
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				log.Println("Received SIGINT. Shutting down...")
				cancel()
				grackeLocksGarbageCollectionWorker.Stop()
				grackeSemaphoresGarbageCollectionWorker.Stop()
				grackeWaitGroupsGarbageCollectionWorker.Stop()
				grpcServer.GracefulStop()
				metricsSrv.Stop()
			case <-ctx.Done():
			}
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		// Grackle API Gateway
		grackleApiGatewayServer := grackle_preview.NewGrackleApiServer(grackleCoreApiClient)
		defer grackleApiGatewayServer.Close()
		gracklepb.RegisterGracklePreviewApiServer(grpcServer, grackleApiGatewayServer)

		log.Println("Starting API Gateway Server...")
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(nonclusteredCmd)

	nonclusteredCmd.PersistentFlags().IntVarP(&nonclusteredCmdCfg.port, "port", "", 0, "Server port")
	err := nonclusteredCmd.MarkPersistentFlagRequired("port")
	if err != nil {
		panic(err)
	}

	nonclusteredCmd.PersistentFlags().IntVarP(&nonclusteredCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	nonclusteredCmd.PersistentFlags().IntVarP(&nonclusteredCmdCfg.shardsCount, "shards", "", 64, "Number of internal shards")

	nonclusteredCmd.PersistentFlags().StringVarP(&nonclusteredCmdCfg.dataDir, "data-dir", "", "", "Base directory for data")
	err = nonclusteredCmd.MarkPersistentFlagRequired("data-dir")
	if err != nil {
		panic(err)
	}

	nonclusteredCmd.PersistentFlags().StringVarP(&nonclusteredCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")
}
