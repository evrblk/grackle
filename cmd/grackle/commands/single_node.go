package commands

import (
	"context"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	grackle_v1beta "github.com/evrblk/grackle/pkg/server/v1beta"
	"github.com/evrblk/grackle/pkg/waitgroups"
	"github.com/evrblk/grackle/pkg/workers"
)

var singleNodeCmdCfg struct {
	gatewayListenAddr    string
	prometheusListenAddr string
	authKeysPath         string
	shardsCount          int
	dataDir              string
	log                  logFlags
}

var singleNodeCmd = &cobra.Command{
	Use:   "single-node",
	Short: "Run Grackle in single-node mode",
	Run: func(cmd *cobra.Command, args []string) {
		baseLogger := setupLogger(singleNodeCmdCfg.log).With("service_name", "single-node")
		baseLogger.Info("Initializing Grackle...")

		lis, err := net.Listen("tcp", singleNodeCmdCfg.gatewayListenAddr)
		if err != nil {
			baseLogger.Error("failed to listen", "error", err, "address", singleNodeCmdCfg.gatewayListenAddr)
			os.Exit(1)
		}

		// Metrics
		workers.RegisterMetrics(prometheus.DefaultRegisterer)
		metricsSrv := metrics.NewMetricsServer(singleNodeCmdCfg.prometheusListenAddr)
		metricsSrv.Start()

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(singleNodeCmdCfg.dataDir, "cores")))
		if err != nil {
			baseLogger.Error("failed to create data store", "error", err)
			os.Exit(1)
		}

		// Node-local registry handing out a stable two-byte prefix per shard, so
		// every core namespaces its data in the shared store without collisions.
		// Replaces the old truncated-hash shard prefix; see honey.ReplicaPrefixRegistry.
		replicaRegistry := honey.NewReplicaPrefixRegistry(dataStore)
		replicaPrefix := func(shardId string) []byte {
			prefix, err := replicaRegistry.GetOrAssignPrefix(shardId)
			if err != nil {
				baseLogger.Error("failed to assign replica prefix", "shard_id", shardId, "error", err)
				os.Exit(1)
			}
			return prefix
		}

		// Middleware
		monitoringMiddleware := middleware.NewMonitoringMiddleware("grackle", baseLogger.With("component", "grpc"))
		monitoringMiddleware.Register(prometheus.DefaultRegisterer)

		unaryInterceptors := []grpc.UnaryServerInterceptor{monitoringMiddleware.Unary}
		if singleNodeCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, middleware.NewAuthenticationMiddleware(singleNodeCmdCfg.authKeysPath, "Grackle").Unary)
		}

		// Grackle single node client
		coresFactory := &coreapis.GrackleNonclusteredApplicationCoresFactory{
			GrackleLocksCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.GrackleLocksCoreApi {
				return locks.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
			GrackleNamespacesCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.GrackleNamespacesCoreApi {
				return namespaces.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
			GrackleSemaphoresCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.GrackleSemaphoresCoreApi {
				return semaphores.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
			GrackleWaitGroupsCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.GrackleWaitGroupsCoreApi {
				return waitgroups.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
			GrackleBarriersCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.GrackleBarriersCoreApi {
				return barriers.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
		}
		grackleCoreApiClient := coreapis.NewGrackleNonclusteredStub(singleNodeCmdCfg.shardsCount, coresFactory, baseLogger.With("component", "core"))

		// Grackle workers
		grackeLocksGarbageCollectionWorker := workers.NewGrackleLocksGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-locks-gc-worker"))
		grackeLocksGarbageCollectionWorker.Start()
		grackeSemaphoresGarbageCollectionWorker := workers.NewGrackleSemaphoresGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-semaphores-gc-worker"))
		grackeSemaphoresGarbageCollectionWorker.Start()
		grackeWaitGroupsGarbageCollectionWorker := workers.NewGrackleWaitGroupsGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-wait-groups-gc-worker"))
		grackeWaitGroupsGarbageCollectionWorker.Start()
		grackeBarriersGarbageCollectionWorker := workers.NewGrackleBarriersGCWorker(grackleCoreApiClient, baseLogger.With("component", "grackle-barriers-gc-worker"))
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
				baseLogger.Info("Received SIGINT. Shutting down...")
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

		baseLogger.Info("Starting API Gateway Server...", "address", singleNodeCmdCfg.gatewayListenAddr)
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(singleNodeCmd)

	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.gatewayListenAddr, "gateway-listen-addr", "", ":8000", "API Gateway bind address")
	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.dataDir, "data-dir", "", "./data", "Base directory for data")
	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.prometheusListenAddr, "prometheus-listen-addr", "", ":2112", "Prometheus metrics bind address")
	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.shardsCount, "shards", "", 64, "Number of internal shards")
	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")

	addLogFlags(singleNodeCmd, &singleNodeCmdCfg.log)
}
