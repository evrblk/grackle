package commands

import (
	"github.com/spf13/cobra"
)

// runCmd represents the base command for running Grackle in different clustered and non-clustered modes
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Grackle",
	Long:  "",
}

func init() {
	rootCmd.AddCommand(runCmd)
}
