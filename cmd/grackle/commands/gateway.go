package commands

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	gracklepb "github.com/evrblk/evrblk-go/grackle/preview"
	"github.com/evrblk/grackle/pkg/grackle"
	grackle_preview "github.com/evrblk/grackle/pkg/preview"
	"github.com/evrblk/monstera"
	"github.com/evrblk/yellowstone-common/metrics"
)

var gatewayCmdConfig struct {
	port               int
	prometheusPort     int
	monsteraConfigPath string
}

var gatewayCmd = &cobra.Command{
	Use:   "gateway",
	Short: "Run Grackle API Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Grackle API Gateway Server...")

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", gatewayCmdConfig.port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Metrics
		metricsSrv := metrics.NewMetricsServer(gatewayCmdConfig.prometheusPort)
		metricsSrv.Start()

		// Load monstera cluster config
		clusterConfig, err := monstera.LoadConfigFromFile(gatewayCmdConfig.monsteraConfigPath)
		if err != nil {
			log.Fatal(err)
		}

		// Create Monstera client
		monsteraClient := monstera.NewMonsteraClient(clusterConfig)
		monsteraClient.Start()

		// Middleware
		//monitoringMiddleware := yellowstone.NewMonitoringMiddleware()

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(
			//monitoringMiddleware.Unary,
			//loggingMiddleware.Unary,
			//authnMiddleware.Unary,
			),
			grpc.ChainStreamInterceptor(),
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
		grackleCoreApiClient := grackle.NewGrackleCoreApiMonsteraStub(monsteraClient, &grackle.GrackleShardKeyCalculator{})
		grackleApiGatewayServer := grackle_preview.NewGrackleApiServer(grackleCoreApiClient)
		defer grackleApiGatewayServer.Close()
		gracklepb.RegisterGracklePreviewApiServer(grpcServer, grackleApiGatewayServer)

		log.Println("Starting API Gateway Server...")
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(gatewayCmd)

	gatewayCmd.PersistentFlags().IntVarP(&gatewayCmdConfig.port, "port", "", 0, "Server port")
	err := gatewayCmd.MarkPersistentFlagRequired("port")
	if err != nil {
		panic(err)
	}

	gatewayCmd.PersistentFlags().IntVarP(&gatewayCmdConfig.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdConfig.monsteraConfigPath, "monstera-config", "", "", "Monstera cluster config path")
	err = gatewayCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}
}
