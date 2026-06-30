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
	"github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/monstera/utils"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	"github.com/evrblk/grackle/pkg/tables"
	"github.com/evrblk/grackle/pkg/waitgroups"
)

var nodeCmdCfg struct {
	prometheusPort     int
	dataDir            string
	monsteraConfigPath string
	nodeId             string
}

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Run Monstera node with Grackle cores",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle Node server...")

		// Metrics
		metricsSrv := metrics.NewMetricsServer(nodeCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Register table prefixes
		registry := honey.NewBaseTableRegistry(1)
		tables.RegisterGracklePrefixes(registry)

		// Load monstera cluster config
		clusterConfig, err := cluster.LoadConfigFromFile(nodeCmdCfg.monsteraConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		node, err := clusterConfig.GetNode(nodeCmdCfg.nodeId)
		if err != nil {
			log.Fatal(err)
		}

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(filepath.Join(nodeCmdCfg.dataDir, "data"))
		if err != nil {
			log.Fatal(err)
		}

		// TODO set timeouts
		monsteraNodeConfig := monstera.DefaultMonsteraNodeConfig
		monsteraNodeConfig.UseInMemoryRaftStore = true

		applicationDescriptors := monstera.ApplicationCoreDescriptors{
			"GrackleLocks": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleLocksCoreAdapter(
						shard.Id, replica.Id,
						locks.NewCore(dataStore, utils.GetTruncatedHash([]byte(shard.Id), 4), shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleNamespaces": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleNamespacesCoreAdapter(
						shard.Id, replica.Id,
						namespaces.NewCore(dataStore, shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleWaitGroups": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleWaitGroupsCoreAdapter(
						shard.Id, replica.Id,
						waitgroups.NewCore(dataStore, utils.GetTruncatedHash([]byte(shard.Id), 4), shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleBarriers": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleBarriersCoreAdapter(
						shard.Id, replica.Id,
						barriers.NewCore(dataStore, utils.GetTruncatedHash([]byte(shard.Id), 4), shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleSemaphores": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
					return coreapis.NewGrackleSemaphoresCoreAdapter(
						shard.Id, replica.Id,
						semaphores.NewCore(dataStore, utils.GetTruncatedHash([]byte(shard.Id), 4), shard.LowerBound, shard.UpperBound))
				},
			},
		}

		transport := grpc.NewGrpcTransport(clusterConfig)

		monsteraNode, err := monstera.NewNode(nodeCmdCfg.dataDir, nodeCmdCfg.nodeId, clusterConfig, applicationDescriptors, monsteraNodeConfig, transport)
		if err != nil {
			log.Fatalf("failed to create Monstera node: %v", err)
		}

		// Starting Monstera node
		monsteraNode.Start()

		// Register node metrics
		err = prometheus.Register(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "monstera_node_ready",
				Help:        "Monstera node is ready",
				ConstLabels: prometheus.Labels{"node": nodeCmdCfg.nodeId},
			},
			func() float64 {
				if monsteraNode.NodeState() == monstera.READY {
					return 1
				} else {
					return 0
				}
			},
		))
		if err != nil {
			log.Fatalf("failed to register node metrics: %v", err)
		}

		monsteraServer := grpc.NewGrpcServer(monsteraNode)

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

		err = monsteraServer.Serve(node.GrpcAddress)
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

	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.monsteraConfigPath, "monstera-config", "", "", "Monstera cluster config path")
	err := nodeCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}
	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.dataDir, "data-dir", "", "", "Base directory for data")
	err = nodeCmd.MarkPersistentFlagRequired("data-dir")
	if err != nil {
		panic(err)
	}
	nodeCmd.PersistentFlags().StringVarP(&nodeCmdCfg.nodeId, "node-id", "", "", "Monstera node ID")
	err = nodeCmd.MarkPersistentFlagRequired("node-id")
	if err != nil {
		panic(err)
	}
}
