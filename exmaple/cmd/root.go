package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "root",
	Short: "root command",
	Long:  "the long command for root command",
	Run: func(cmd *cobra.Command, args []string) {
		// print help for cli
	},
}

func initialize() {
	initializeAdd()
	initializeGet()
	rootCmd.AddCommand(addCommand, getCommand)
}

func Execute() {
	initialize()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
