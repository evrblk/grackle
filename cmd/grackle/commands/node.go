package commands

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/evrblk/grackle/pkg/grackle"
	"github.com/evrblk/monstera"
	"github.com/evrblk/yellowstone-common/metrics"
)

var nodeCmdConfig struct {
	monsteraPort       int
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
		metricsSrv := metrics.NewMetricsServer(nodeCmdConfig.prometheusPort)
		metricsSrv.Start()

		// Load monstera cluster config
		clusterConfig, err := monstera.LoadConfigFromFile(nodeCmdConfig.monsteraConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		// Create shared Badger store for application cores
		dataStore := monstera.NewBadgerStore(filepath.Join(nodeCmdConfig.dataDir, "data"))

		// TODO set timeouts
		monsteraNodeConfig := monstera.DefaultMonsteraNodeConfig

		applicationDescriptors := monstera.ApplicationCoreDescriptors{
			"GrackleLocks": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *monstera.Shard, replica *monstera.Replica) monstera.ApplicationCore {
					return grackle.NewGrackleLocksCoreAdapter(
						shard.Id, replica.Id,
						grackle.NewLocksCore(dataStore, shard.GlobalIndexPrefix, shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleNamespaces": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *monstera.Shard, replica *monstera.Replica) monstera.ApplicationCore {
					return grackle.NewGrackleNamespacesCoreAdapter(
						shard.Id, replica.Id,
						grackle.NewNamespacesCore(dataStore, shard.GlobalIndexPrefix, shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleWaitGroups": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *monstera.Shard, replica *monstera.Replica) monstera.ApplicationCore {
					return grackle.NewGrackleWaitGroupsCoreAdapter(
						shard.Id, replica.Id,
						grackle.NewWaitGroupsCore(dataStore, shard.GlobalIndexPrefix, shard.LowerBound, shard.UpperBound))
				},
			},
			"GrackleSemaphores": {
				RestoreSnapshotOnStart: false,
				CoreFactoryFunc: func(shard *monstera.Shard, replica *monstera.Replica) monstera.ApplicationCore {
					return grackle.NewGrackleSemaphoresCoreAdapter(
						shard.Id, replica.Id,
						grackle.NewSemaphoresCore(dataStore, shard.GlobalIndexPrefix, shard.LowerBound, shard.UpperBound))
				},
			},
		}

		monsteraNode, err := monstera.NewNode(nodeCmdConfig.dataDir, nodeCmdConfig.nodeId, clusterConfig, applicationDescriptors, monsteraNodeConfig)
		if err != nil {
			log.Fatalf("failed to create Monstera node: %v", err)
		}

		// TODO
		// Middleware
		//monitoringMiddleware := yellowstone.NewMonitoringMiddleware()
		//monsteraMetricsMiddleware := yellowstone.NewMonsteraMetricsMiddleware(nodeCmdConfig.nodeId)

		// Starting Monstera node
		monsteraNode.Start()

		// Starting Monstera gRPC server
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nodeCmdConfig.monsteraPort))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Register node metrics
		err = prometheus.Register(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "monstera_node_ready",
				Help:        "Monstera node is ready",
				ConstLabels: prometheus.Labels{"node_id": nodeCmdConfig.nodeId},
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

		monsteraServer := monstera.NewMonsteraServer(monsteraNode)

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(
			//monitoringMiddleware.Unary,
			//monsteraMetricsMiddleware.Unary,
			),
			grpc.MaxRecvMsgSize(50*1024*1024),
		)
		monstera.RegisterMonsteraApiServer(grpcServer, monsteraServer)

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
				grpcServer.Stop()
				monsteraNode.Stop()
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

		err = grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Monstera gRPC server stopped: %s", err)
		} else {
			log.Printf("Monstera gRPC server stopped")
		}

		cleanupDone.Wait()

		log.Printf("Exiting...")
	},
}

func init() {
	runCmd.AddCommand(nodeCmd)

	nodeCmd.PersistentFlags().IntVarP(&nodeCmdConfig.monsteraPort, "monstera-port", "", 0, "Monstera server port")
	err := nodeCmd.MarkPersistentFlagRequired("monstera-port")
	if err != nil {
		panic(err)
	}

	nodeCmd.PersistentFlags().IntVarP(&nodeCmdConfig.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	nodeCmd.PersistentFlags().StringVarP(&nodeCmdConfig.monsteraConfigPath, "monstera-config", "", "", "Monstera cluster config path")
	err = nodeCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}

	nodeCmd.PersistentFlags().StringVarP(&nodeCmdConfig.dataDir, "data-dir", "", "", "Base directory for data")
	err = nodeCmd.MarkPersistentFlagRequired("data-dir")
	if err != nil {
		panic(err)
	}

	nodeCmd.PersistentFlags().StringVarP(&nodeCmdConfig.nodeId, "node-id", "", "", "Monstera node ID")
	err = nodeCmd.MarkPersistentFlagRequired("node-id")
	if err != nil {
		panic(err)
	}
}
