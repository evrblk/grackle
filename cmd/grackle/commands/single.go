package commands

import (
	"github.com/spf13/cobra"
)

var singleCmdConfig struct {
	prometheusPort     int
	monsteraConfigPath string
}

var singleCmd = &cobra.Command{
	Use:   "single",
	Short: "Run Grackle in all-in-one non-clustered mode",
	Run: func(cmd *cobra.Command, args []string) {
		// TODO
	},
}

func init() {
	runCmd.AddCommand(singleCmd)

	singleCmd.PersistentFlags().IntVarP(&singleCmdConfig.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	singleCmd.PersistentFlags().StringVarP(&singleCmdConfig.monsteraConfigPath, "monstera-config", "", "", "Monstera cluster config path")
	err := singleCmd.MarkPersistentFlagRequired("monstera-config")
	if err != nil {
		panic(err)
	}
}
