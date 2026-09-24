package commands

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	monstera_grpc "github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	"github.com/evrblk/grackle/pkg/waitgroups"
)

var nodeCmdCfg struct {
	prometheusListenAddr  string
	dataDir               string
	monsteraListenAddress string
	log                   logFlags
	coreLog               nodeCoreLogFlags
}

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Run Monstera node with Grackle cores",
	Run: func(cmd *cobra.Command, args []string) {
		baseLogger := setupLogger(nodeCmdCfg.log).With("service_name", "node")
		baseLogger.Info("Initializing Grackle Node server...", "address", nodeCmdCfg.monsteraListenAddress)

		// Metrics
		monstera.RegisterMetrics(prometheus.DefaultRegisterer)
		coreapis.RegisterMetrics(prometheus.DefaultRegisterer)
		metricsSrv := metrics.NewMetricsServer(nodeCmdCfg.prometheusListenAddr)
		metricsSrv.Start()

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(nodeCmdCfg.dataDir, "cores")))
		if err != nil {
			baseLogger.Error(err.Error())
			os.Exit(1)
		}

		// Node-local registry handing out a stable two-byte prefix per replica, so
		// every core namespaces its data in the shared store without collisions.
		replicaRegistry := honey.NewReplicaPrefixRegistry(dataStore)
		replicaPrefix := func(replicaId string) []byte {
			prefix, err := replicaRegistry.GetOrAssignPrefix(replicaId)
			if err != nil {
				baseLogger.Error("failed to assign replica prefix", "replica_id", replicaId, "error", err)
				os.Exit(1)
			}
			return prefix
		}

		applicationDescriptors := monstera.ApplicationCoreDescriptors{
			"GrackleLocks": {
				CoreType: monstera.CoreTypePersistedExclusive,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleLocksCoreAdapter(
						replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
						locks.NewCore(dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
				},
			},
			"GrackleNamespaces": {
				CoreType: monstera.CoreTypePersistedExclusive,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleNamespacesCoreAdapter(
						replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
						namespaces.NewCore(dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
				},
			},
			"GrackleWaitGroups": {
				CoreType: monstera.CoreTypePersistedExclusive,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleWaitGroupsCoreAdapter(
						replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
						waitgroups.NewCore(dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
				},
			},
			"GrackleBarriers": {
				CoreType: monstera.CoreTypePersistedExclusive,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleBarriersCoreAdapter(
						replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
						barriers.NewCore(dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
				},
			},
			"GrackleSemaphores": {
				CoreType: monstera.CoreTypePersistedExclusive,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleSemaphoresCoreAdapter(
						replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
						semaphores.NewCore(dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
				},
			},
		}

		transport := monstera_grpc.NewDataPlaneClient()

		coreLogPolicy, coreLogDestination := setupCoreLogging(nodeCmdCfg.coreLog)
		// TODO set timeouts
		monsteraNodeConfig := monstera.DefaultMonsteraNodeConfig
		monsteraNodeConfig.CoreLogPolicy = coreLogPolicy
		monsteraNodeConfig.CoreLogDestination = coreLogDestination

		monsteraNode, err := monstera.NewNode(nodeCmdCfg.dataDir, applicationDescriptors, monsteraNodeConfig, transport)
		if err != nil {
			baseLogger.Error("failed to create Monstera node", "error", err)
			os.Exit(1)
		}

		monsteraNode.Start()

		monsteraServer := monstera_grpc.NewGrpcServer(monsteraNode, monstera_grpc.WithServerLogger(baseLogger.With("component", "monstera-grpc")))

		cleanupDone := &sync.WaitGroup{}
		cleanupDone.Add(1)

		ctx, cancel := context.WithCancel(context.Background())
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				baseLogger.Info("Received SIGINT. Shutting down...")
				cancel()
				monsteraNode.Stop()
				monsteraServer.Stop()
				dataStore.Close()
				metricsSrv.Stop()
			case <-ctx.Done():
			}
			cleanupDone.Done()
			baseLogger.Info("Cleanup done")
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		err = monsteraServer.Serve(nodeCmdCfg.monsteraListenAddress)
		if err != nil {
			baseLogger.Info("Monstera server stopped", "error", err)
		} else {
			baseLogger.Info("Monstera server stopped")
		}

		cleanupDone.Wait()

		baseLogger.Info("Exiting...")
	},
}

func init() {
	runCmd.AddCommand(nodeCmd)

	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.prometheusListenAddr, "prometheus-listen-addr", "", ":2112", "Prometheus metrics bind address")
	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.monsteraListenAddress, "monstera-listen-addr", "", ":9000", "Monstera gRPC bind address")
	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.dataDir, "data-dir", "", "./data", "Base directory for data")

	addLogFlags(nodeCmd, &nodeCmdCfg.log)
	addNodeCoreLogFlags(nodeCmd, &nodeCmdCfg.coreLog)
}
