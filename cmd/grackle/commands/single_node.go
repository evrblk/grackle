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

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	grackle_v1beta "github.com/evrblk/grackle/pkg/server/v1beta"
	"github.com/evrblk/grackle/pkg/tables"
	"github.com/evrblk/grackle/pkg/waitgroups"
	"github.com/evrblk/grackle/pkg/workers"
)

var singleNodeCmdCfg struct {
	port           int
	prometheusPort int
	authKeysPath   string
	shardsCount    int
	dataDir        string
}

var singleNodeCmd = &cobra.Command{
	Use:   "single-node",
	Short: "Run Grackle in single-node mode",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle...")

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", singleNodeCmdCfg.port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Metrics
		grackle_v1beta.RegisterMetrics()
		metricsSrv := metrics.NewMetricsServer(singleNodeCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Register table prefixes
		registry := honey.NewBaseTableRegistry(1)
		tables.RegisterGracklePrefixes(registry)

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(filepath.Join(singleNodeCmdCfg.dataDir, "data"))
		if err != nil {
			log.Fatalf("failed to create data store: %v", err)
		}

		// Middleware
		unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
		if singleNodeCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, grackle_v1beta.NewAuthenticationMiddleware(singleNodeCmdCfg.authKeysPath).Unary)
		}

		// Grackle single node client
		coresFactory := &coreapis.GrackleNonclusteredApplicationCoresFactory{
			GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleLocksCoreApi {
				return locks.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
			GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleNamespacesCoreApi {
				return namespaces.NewCore(dataStore, lowerBound, upperBound)
			},
			GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleSemaphoresCoreApi {
				return semaphores.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
			GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleWaitGroupsCoreApi {
				return waitgroups.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
			GrackleBarriersCoreFactoryFunc: func(shardId string, lowerBound []byte, upperBound []byte) coreapis.GrackleBarriersCoreApi {
				return barriers.NewCore(dataStore, utils.GetTruncatedHash([]byte(shardId), 4), lowerBound, upperBound)
			},
		}
		grackleCoreApiClient := coreapis.NewGrackleNonclusteredStub(singleNodeCmdCfg.shardsCount, coresFactory)

		// Grackle workers
		grackeLocksGarbageCollectionWorker := workers.NewGrackleLocksGCWorker(grackleCoreApiClient)
		grackeLocksGarbageCollectionWorker.Start()
		grackeSemaphoresGarbageCollectionWorker := workers.NewGrackleSemaphoresGCWorker(grackleCoreApiClient)
		grackeSemaphoresGarbageCollectionWorker.Start()
		grackeWaitGroupsGarbageCollectionWorker := workers.NewGrackleWaitGroupsGCWorker(grackleCoreApiClient)
		grackeWaitGroupsGarbageCollectionWorker.Start()
		grackeBarriersGarbageCollectionWorker := workers.NewGrackleBarriersGCWorker(grackleCoreApiClient)
		grackeBarriersGarbageCollectionWorker.Start()

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
				grackeBarriersGarbageCollectionWorker.Stop()
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
		grackleApiGatewayServer := grackle_v1beta.NewGrackleApiServer(grackleCoreApiClient)
		defer grackleApiGatewayServer.Close()
		gracklepb.RegisterGrackleApiServer(grpcServer, grackleApiGatewayServer)

		log.Println("Starting API Gateway Server...")
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(singleNodeCmd)

	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.port, "port", "", 0, "Server port")
	err := singleNodeCmd.MarkPersistentFlagRequired("port")
	if err != nil {
		panic(err)
	}

	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.shardsCount, "shards", "", 64, "Number of internal shards")

	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.dataDir, "data-dir", "", "", "Base directory for data")
	err = singleNodeCmd.MarkPersistentFlagRequired("data-dir")
	if err != nil {
		panic(err)
	}

	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")
}
