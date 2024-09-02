package cmd

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "kafka-proto-cli",
	Short: "Utility to produce messages to Kafka",
}

func init() {
	cobra.OnInitialize()
	rootCmd.AddCommand(produceCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
