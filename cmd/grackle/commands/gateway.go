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
	"github.com/evrblk/monstera/cluster"
	monstrea_grpc "github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/coreapis"
	grackle_preview "github.com/evrblk/grackle/pkg/server/preview"
)

var gatewayCmdCfg struct {
	port               int
	prometheusPort     int
	monsteraConfigPath string
	authKeysPath       string
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
		grackle_preview.RegisterMetrics()
		metricsSrv := metrics.NewMetricsServer(gatewayCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Load monstera cluster config
		clusterConfig, err := cluster.LoadConfigFromFile(gatewayCmdCfg.monsteraConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		// Create transport
		transport := monstrea_grpc.NewGrpcTransport(clusterConfig)

		// Create Monstera client
		monsteraClient := monstera.NewMonsteraClient(clusterConfig, transport, monstera.DefaultClientConfig())
		monsteraClient.Start()

		// Middleware
		unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
		if gatewayCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, grackle_preview.NewAuthenticationMiddleware(gatewayCmdCfg.authKeysPath).Unary)
		}

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
				grpcServer.GracefulStop()
				monsteraClient.Stop()
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
		grackleApiGatewayServer := grackle_preview.NewGrackleApiServer(grackleCoreApiClient)
		defer grackleApiGatewayServer.Close()
		gracklepb.RegisterGracklePreviewApiServer(grpcServer, grackleApiGatewayServer)

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

	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.monsteraConfigPath, "monstera-config", "", "", "Monstera cluster config path")
	err = gatewayCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}

	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")
}
