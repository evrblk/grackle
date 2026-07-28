package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"syscall"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/monstera/transport/local"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	grpc_server "google.golang.org/grpc"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/evrblk/grackle/pkg/barriers"
	"github.com/evrblk/grackle/pkg/coreapis"
	"github.com/evrblk/grackle/pkg/locks"
	"github.com/evrblk/grackle/pkg/namespaces"
	"github.com/evrblk/grackle/pkg/semaphores"
	grackle_v1beta "github.com/evrblk/grackle/pkg/server/v1beta"
	"github.com/evrblk/grackle/pkg/waitgroups"
)

var (
	configPath     = flag.String("config", "./cluster_config.json", "Path to cluster config file")
	baseDataDir    = flag.String("data-dir", "./.data", "Base directory for node data")
	prometheusPort = flag.Int("prometheus-port", 2112, "Prometheus metrics port")
	cpuProfile     = flag.String("cpu-profile", "", "Write CPU profile to file")
	transportType  = flag.String("transport", "grpc", "Transport type: 'grpc' or 'local'")
	gatewayPort    = flag.Int("gateway-port", 0, "Gateway port for client connections (0 = disabled)")
)

type nodeRunner struct {
	nodeID          string
	dataDir         string
	clusterConfig   *cluster.Config
	monsteraNode    *monstera.Node
	monsteraServer  *grpc.GrpcServer
	dataStore       *store.BadgerStore
	replicaRegistry *honey.ReplicaPrefixRegistry
}

func newNodeRunner(nodeID string, baseDataDir string, clusterConfig *cluster.Config) (*nodeRunner, error) {
	dataDir := filepath.Join(baseDataDir, nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory for %s: %w", nodeID, err)
	}

	// Create shared Badger store for application cores
	dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(dataDir, "cores")))
	if err != nil {
		return nil, fmt.Errorf("failed to create data store for %s: %w", nodeID, err)
	}

	return &nodeRunner{
		nodeID:        nodeID,
		dataDir:       dataDir,
		clusterConfig: clusterConfig,
		dataStore:     dataStore,
		// Node-local registry handing out a stable two-byte prefix per replica,
		// replacing the old truncated-hash shard prefix. See honey.ReplicaPrefixRegistry.
		replicaRegistry: honey.NewReplicaPrefixRegistry(dataStore),
	}, nil
}

func (nr *nodeRunner) start(transport transport.DataPlane, useGrpc bool) error {
	log.Printf("Starting node %s...", nr.nodeID)

	node, err := nr.clusterConfig.GetNode(nr.nodeID)
	if err != nil {
		return fmt.Errorf("failed to get node config for %s: %w", nr.nodeID, err)
	}

	monsteraNodeConfig := monstera.DefaultMonsteraNodeConfig
	monsteraNodeConfig.UseInMemoryRaftStore = true

	replicaPrefix := func(replicaId string) []byte {
		prefix, err := nr.replicaRegistry.GetOrAssignPrefix(replicaId)
		if err != nil {
			log.Fatalf("failed to assign replica prefix for replica %s: %v", replicaId, err)
		}
		return prefix
	}

	applicationDescriptors := monstera.ApplicationCoreDescriptors{
		"GrackleLocks": {
			CoreType: monstera.CoreTypePersistedExclusive,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return coreapis.NewGrackleLocksCoreAdapter(
					replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
					locks.NewCore(nr.dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
			},
		},
		"GrackleNamespaces": {
			CoreType: monstera.CoreTypePersistedExclusive,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return coreapis.NewGrackleNamespacesCoreAdapter(
					replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
					namespaces.NewCore(nr.dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
			},
		},
		"GrackleWaitGroups": {
			CoreType: monstera.CoreTypePersistedExclusive,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return coreapis.NewGrackleWaitGroupsCoreAdapter(
					replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
					waitgroups.NewCore(nr.dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
			},
		},
		"GrackleBarriers": {
			CoreType: monstera.CoreTypePersistedExclusive,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return coreapis.NewGrackleBarriersCoreAdapter(
					replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
					barriers.NewCore(nr.dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
			},
		},
		"GrackleSemaphores": {
			CoreType: monstera.CoreTypePersistedExclusive,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return coreapis.NewGrackleSemaphoresCoreAdapter(
					replica.NodeId, shard.Id, replica.Id, shard.LowerKey(), shard.UpperKey(),
					semaphores.NewCore(nr.dataStore, replicaPrefix(replica.Id), shard.LowerKey(), shard.UpperKey()))
			},
		},
	}

	monsteraNode, err := monstera.NewNode(nr.dataDir, applicationDescriptors, monsteraNodeConfig, transport)
	if err != nil {
		return fmt.Errorf("failed to create Monstera node %s: %w", nr.nodeID, err)
	}

	nr.monsteraNode = monsteraNode

	// Starting Monstera node. A fresh data dir comes up UNPROVISIONED; bootstrap it
	// in-process with the cluster config (mirrors an admin Bootstrap over the wire).
	monsteraNode.Start()
	if monsteraNode.NodeState() == monstera.NodeStateUnprovisioned {
		if err := monsteraNode.Bootstrap(context.Background(), nr.nodeID, nr.clusterConfig); err != nil {
			return fmt.Errorf("failed to bootstrap Monstera node %s: %w", nr.nodeID, err)
		}
	}

	// The monstera_node_ready gauge is emitted by the node itself (registered via
	// monstera.RegisterMetrics in main).

	// Only start gRPC server when using gRPC transport
	if useGrpc {
		nr.monsteraServer = grpc.NewGrpcServer(monsteraNode)

		// Start gRPC server in a goroutine
		go func() {
			log.Printf("Starting gRPC server for %s on %s", nr.nodeID, node.GrpcAddress)
			err := nr.monsteraServer.Serve(node.GrpcAddress)
			if err != nil {
				log.Printf("Monstera server for %s stopped: %v", nr.nodeID, err)
			} else {
				log.Printf("Monstera server for %s stopped", nr.nodeID)
			}
		}()
	}

	return nil
}

func (nr *nodeRunner) stop() {
	log.Printf("Stopping node %s...", nr.nodeID)
	if nr.monsteraNode != nil {
		nr.monsteraNode.Stop()
	}
	if nr.monsteraServer != nil {
		nr.monsteraServer.Stop()
	}
	if nr.dataStore != nil {
		nr.dataStore.Close()
	}
}

func main() {
	flag.Parse()

	// CPU profiling
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Failed to create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("Failed to start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		log.Printf("CPU profiling enabled, writing to %s", *cpuProfile)
	}

	log.Println("Initializing Debug Cluster...")

	// Metrics
	monstera.RegisterMetrics(prometheus.DefaultRegisterer)
	coreapis.RegisterMetrics(prometheus.DefaultRegisterer)
	metricsSrv := metrics.NewMetricsServer(*prometheusPort)
	metricsSrv.Start()
	defer metricsSrv.Stop()

	// Validate and configure transport
	useGrpc := true
	var nodeTransport transport.DataPlane

	switch *transportType {
	case "grpc":
		log.Println("Using gRPC transport")
		useGrpc = true
		// Each node will use gRPC transport (created per-node in start())
	case "local":
		log.Println("Using local in-memory transport")
		useGrpc = false
		nodeTransport = local.NewLocalTransport()
		defer nodeTransport.Close()
	default:
		log.Fatalf("Invalid transport type: %s (must be 'grpc' or 'local')", *transportType)
	}

	// Load monstera cluster config
	clusterConfig, err := cluster.LoadConfigFromFile(*configPath)
	if err != nil {
		log.Fatalf("Failed to load cluster config: %v", err)
	}

	// Create node runners for all nodes in the cluster config
	nodes := clusterConfig.GetNodes()
	log.Printf("Creating runners for %d nodes from cluster config", len(nodes))
	runners := make([]*nodeRunner, 0, len(nodes))

	for _, node := range nodes {
		nodeID := node.GetId()
		runner, err := newNodeRunner(nodeID, *baseDataDir, clusterConfig)
		if err != nil {
			log.Fatalf("Failed to create node runner for %s: %v", nodeID, err)
		}
		runners = append(runners, runner)
	}

	// Start all nodes
	for _, runner := range runners {
		// Create per-node transport for gRPC, or use shared local transport
		var trans transport.DataPlane
		if useGrpc {
			trans = grpc.NewDataPlaneClient()
		} else {
			trans = nodeTransport
		}

		if err := runner.start(trans, useGrpc); err != nil {
			log.Fatalf("Failed to start node %s: %v", runner.nodeID, err)
		}

		// For local transport, register each node after it starts
		if !useGrpc {
			localTrans := nodeTransport.(*local.LocalTransport)
			localTrans.Register(runner.monsteraNode)
		}
	}

	log.Println("All nodes started successfully!")

	// Start gateway if port is specified
	var monsteraClient *monstera.Client
	var grpcServer *grpc_server.Server
	var grackleApiGatewayServer *grackle_v1beta.GrackleApiServer

	if *gatewayPort > 0 {
		log.Printf("Starting gateway on port %d...", *gatewayPort)

		// Register gateway metrics
		grackle_v1beta.RegisterMetrics()

		// Create listener
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *gatewayPort))
		if err != nil {
			log.Fatalf("Failed to listen on gateway port: %v", err)
		}

		// Create transport for the client (reuse the same type as nodes)
		var clientTransport transport.DataPlane
		if useGrpc {
			clientTransport = grpc.NewDataPlaneClient()
		} else {
			clientTransport = nodeTransport
		}

		// Create Monstera client. The debug cluster runs in-process and already has
		// the config, so feed it a static provider (no polling needed).
		monsteraClient = monstera.NewMonsteraClient(monstera.NewStaticClusterConfigProvider(clusterConfig), clientTransport, monstera.DefaultClientConfig())
		if err := monsteraClient.Start(context.Background()); err != nil {
			log.Fatalf("Failed to start monstera client: %v", err)
		}

		// Create gRPC server
		grpcServer = grpc_server.NewServer()

		// Create Grackle API Gateway
		grackleCoreApiClient := coreapis.NewGrackleMonsteraStub(monsteraClient)
		grackleApiGatewayServer = grackle_v1beta.NewGrackleApiServer(grackleCoreApiClient)
		gracklepb.RegisterGrackleApiServer(grpcServer, grackleApiGatewayServer)

		// Start serving in a goroutine
		go func() {
			log.Printf("Gateway listening on port %d", *gatewayPort)
			if err := grpcServer.Serve(lis); err != nil {
				log.Printf("Gateway server stopped: %v", err)
			}
		}()
	}

	// Setup signal handling for graceful shutdown
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

			// Stop gateway if running
			if grpcServer != nil {
				log.Println("Stopping gateway...")
				grpcServer.GracefulStop()
			}
			if grackleApiGatewayServer != nil {
				grackleApiGatewayServer.Close()
			}
			if monsteraClient != nil {
				monsteraClient.Stop()
			}

			// Stop all nodes
			for _, runner := range runners {
				runner.stop()
			}
		case <-ctx.Done():
		}
		cleanupDone.Done()
		log.Printf("Cleanup done")
	}()
	defer func() {
		signal.Stop(c)
		cancel()
	}()

	cleanupDone.Wait()

	log.Printf("Exiting...")
}
