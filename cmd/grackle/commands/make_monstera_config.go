package commands

import (
	"github.com/samber/lo/mutable"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/evrblk/monstera"
)

var (
	makeMonsteraConfigCmdCfg struct {
		output            string
		replicationFactor int
		nodes             []string
	}
)

var makeMonsteraConfigCmd = &cobra.Command{
	Use:   "make-monstera-cluster",
	Short: "Make Monstera cluster config file",
	Run: func(cmd *cobra.Command, args []string) {
		if makeMonsteraConfigCmdCfg.replicationFactor < 3 {
			log.Fatal("Replication factor must be greater than or equal to 3")
		}

		if len(makeMonsteraConfigCmdCfg.nodes) < 3 {
			log.Fatal("Must be at least 3 nodes")
		}

		if err := os.MkdirAll(filepath.Dir(makeMonsteraConfigCmdCfg.output), os.ModePerm); err != nil {
			log.Fatal(err)
		}

		clusterConfig := monstera.CreateEmptyConfig()

		nodeIds := createNodes(clusterConfig, makeMonsteraConfigCmdCfg.nodes)

		for _, a := range applications {
			createApplication(clusterConfig, a.Name, a.Implementation, a.ShardsCount, makeMonsteraConfigCmdCfg.replicationFactor, nodeIds)
		}

		if err := monstera.WriteConfigToFile(clusterConfig, makeMonsteraConfigCmdCfg.output); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(makeMonsteraConfigCmd)

	makeMonsteraConfigCmd.PersistentFlags().StringVarP(&makeMonsteraConfigCmdCfg.output, "output", "o", "", "Output path for Monstera cluster config (.json or .pb)")
	err := makeMonsteraConfigCmd.MarkPersistentFlagRequired("output")
	if err != nil {
		panic(err)
	}

	makeMonsteraConfigCmd.PersistentFlags().IntVarP(&makeMonsteraConfigCmdCfg.replicationFactor, "replication-factor", "r", 3, "Number of replicas, minimum 3")

	makeMonsteraConfigCmd.PersistentFlags().StringArrayVarP(&makeMonsteraConfigCmdCfg.nodes, "node", "n", []string{}, "List of Monstera nodes (host:port), minimum 3")
	err = makeMonsteraConfigCmd.MarkPersistentFlagRequired("node")
	if err != nil {
		panic(err)
	}
}

type Application struct {
	Name           string
	Implementation string
	ShardsCount    int
}

var (
	applications = []Application{
		{
			Name:           "GrackleLocks",
			Implementation: "GrackleLocks",
			ShardsCount:    16,
		},
		{
			Name:           "GrackleSemaphores",
			Implementation: "GrackleSemaphores",
			ShardsCount:    16,
		},
		{
			Name:           "GrackleWaitGroups",
			Implementation: "GrackleWaitGroups",
			ShardsCount:    16,
		},
		{
			Name:           "GrackleNamespaces",
			Implementation: "GrackleNamespaces",
			ShardsCount:    8,
		},
	}
)

func createApplication(clusterConfig *monstera.ClusterConfig, name string, implementation string, shardsCount int, replicationFactor int, nodeIds []string) {
	_, err := clusterConfig.CreateApplication(name, implementation, int32(replicationFactor))
	if err != nil {
		log.Fatal(err)
	}

	step := 256 / shardsCount
	for i := 0; i < shardsCount; i++ {
		shard, err := clusterConfig.CreateShard(name, []byte{byte(step * i), 0x00, 0x00, 0x00}, []byte{byte(step*(i+1)) - 1, 0xff, 0xff, 0xff}, "")
		if err != nil {
			log.Fatal(err)
		}

		shuffledIds := make([]string, len(nodeIds))
		copy(shuffledIds, nodeIds)
		mutable.Shuffle(shuffledIds)

		for j := 0; j < replicationFactor; j++ {
			_, err := clusterConfig.CreateReplica(name, shard.Id, shuffledIds[j])
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func createNodes(clusterConfig *monstera.ClusterConfig, nodes []string) []string {
	result := make([]string, 0, len(nodes))
	for _, n := range nodes {
		node, err := clusterConfig.CreateNode(n)
		if err != nil {
			log.Fatal(err)
		}
		result = append(result, node.Id)
	}
	return result
}
