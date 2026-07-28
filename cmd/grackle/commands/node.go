package commands

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	monstrea_grpc "github.com/evrblk/monstera/transport/grpc"
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
	prometheusPort int
	dataDir        string
	listenAddress  string
}

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Run Monstera node with Grackle cores",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle Node server...")

		// Metrics
		monstera.RegisterMetrics(prometheus.DefaultRegisterer)
		coreapis.RegisterMetrics(prometheus.DefaultRegisterer)
		metricsSrv := metrics.NewMetricsServer(nodeCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(nodeCmdCfg.dataDir, "cores")))
		if err != nil {
			log.Fatal(err)
		}

		// Node-local registry handing out a stable two-byte prefix per replica, so
		// every core namespaces its data in the shared store without collisions.
		replicaRegistry := honey.NewReplicaPrefixRegistry(dataStore)
		replicaPrefix := func(replicaId string) []byte {
			prefix, err := replicaRegistry.GetOrAssignPrefix(replicaId)
			if err != nil {
				log.Fatalf("failed to assign replica prefix for replica %s: %v", replicaId, err)
			}
			return prefix
		}

		applicationDescriptors := monstera.ApplicationCoreDescriptors{
			// All grackle cores are fully re-keyed under shard-unique prefixes
			// with a portable, bounds-filtered Restore (fenestra): split-ready.
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

		transport := monstrea_grpc.NewDataPlaneClient()

		// TODO set timeouts
		monsteraNodeConfig := monstera.DefaultMonsteraNodeConfig

		monsteraNode, err := monstera.NewNode(nodeCmdCfg.dataDir, applicationDescriptors, monsteraNodeConfig, transport)
		if err != nil {
			log.Fatalf("failed to create Monstera node: %v", err)
		}

		monsteraNode.Start()

		monsteraServer := monstrea_grpc.NewGrpcServer(monsteraNode)

		cleanupDone := &sync.WaitGroup{}
		cleanupDone.Add(1)

		ctx, cancel := context.WithCancel(context.Background())
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				log.Println("Received SIGINT. Shutting down...")
				cancel()
				monsteraNode.Stop()
				monsteraServer.Stop()
				dataStore.Close()
				metricsSrv.Stop()
			case <-ctx.Done():
			}
			cleanupDone.Done()
			log.Printf("Cleanup done")
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		err = monsteraServer.Serve(nodeCmdCfg.listenAddress)
		if err != nil {
			log.Printf("Monstera server stopped: %s", err)
		} else {
			log.Printf("Monstera server stopped")
		}

		cleanupDone.Wait()

		log.Printf("Exiting...")
	},
}

func init() {
	runCmd.AddCommand(nodeCmd)

	nodeCmd.PersistentFlags().IntVarP(&nodeCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.dataDir, "data-dir", "", "", "Base directory for data")
	err := nodeCmd.MarkPersistentFlagRequired("data-dir")
	if err != nil {
		panic(err)
	}
	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.listenAddress, "listen", "", "", "gRPC bind address")
	err = nodeCmd.MarkPersistentFlagRequired("listen")
	if err != nil {
		panic(err)
	}
}
