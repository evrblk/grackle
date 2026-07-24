package commands

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/evrblk/monstera"
	monstrea_grpc "github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	gracklepb "github.com/evrblk/evrblk-go/grackle/v1beta"
	"github.com/evrblk/grackle/pkg/coreapis"
	grackle_v1beta "github.com/evrblk/grackle/pkg/server/v1beta"
)

var gatewayCmdCfg struct {
	port           int
	prometheusPort int
	nodes          monsteraNodesFlags
	authKeysPath   string
}

var gatewayCmd = &cobra.Command{
	Use:   "gateway",
	Short: "Run Grackle API Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle API Gateway Server...")

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", gatewayCmdCfg.port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Metrics
		grackle_v1beta.RegisterMetrics()
		metricsSrv := metrics.NewMetricsServer(gatewayCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Node discovery + polling config provider: the gateway learns the cluster
		// config from the cluster itself and refreshes as the topology changes.
		discovery, err := buildNodeDiscovery(gatewayCmdCfg.nodes)
		if err != nil {
			log.Fatal(err)
		}
		adminClient := monstrea_grpc.NewAdminClient()
		provider := monstera.NewPollingClusterConfigProvider(discovery, adminClient, monstera.PollingOptions{})

		// Data plane + Monstera client
		transport := monstrea_grpc.NewDataPlaneClient()
		monsteraClient := monstera.NewMonsteraClient(provider, transport, monstera.DefaultClientConfig())

		ctx, cancel := context.WithCancel(context.Background())
		if err := monsteraClient.Start(ctx); err != nil {
			log.Fatalf("failed to start monstera client: %v", err)
		}

		// Middleware
		unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
		if gatewayCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, grackle_v1beta.NewAuthenticationMiddleware(gatewayCmdCfg.authKeysPath).Unary)
		}

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
		)

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				log.Println("Received SIGINT. Shutting down...")
				cancel()
				grpcServer.GracefulStop()
				monsteraClient.Stop()
				adminClient.Close()
				metricsSrv.Stop()
			case <-ctx.Done():
			}
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		// Grackle API Gateway
		grackleCoreApiClient := coreapis.NewGrackleMonsteraStub(monsteraClient)
		grackleApiGatewayServer := grackle_v1beta.NewGrackleApiServer(grackleCoreApiClient)
		defer grackleApiGatewayServer.Close()
		gracklepb.RegisterGrackleApiServer(grpcServer, grackleApiGatewayServer)

		log.Println("Starting API Gateway Server...")
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(gatewayCmd)

	gatewayCmd.PersistentFlags().IntVarP(&gatewayCmdCfg.port, "port", "", 0, "Server port")
	err := gatewayCmd.MarkPersistentFlagRequired("port")
	if err != nil {
		panic(err)
	}

	gatewayCmd.PersistentFlags().IntVarP(&gatewayCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	addMonsteraNodesFlags(gatewayCmd, &gatewayCmdCfg.nodes)

	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")
}
