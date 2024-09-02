package cmd

import (
	"github.com/spf13/cobra"
	"kafka-protobuf-cli/src/config"
	"kafka-protobuf-cli/src/producer"
)

var produceCmd = &cobra.Command{
	Use:   "producer",
	Short: "Sends protobuf to Kafka",
	Run:   run,
}

func init() {
	produceCmd.Flags().StringSliceP("brokers", "b", []string{}, "Kafka broker addresses separated by comma")
	produceCmd.Flags().StringP("proto", "p", "", "Proto file path")
	produceCmd.Flags().StringP("name", "n", "", "Full proto message name")
	produceCmd.Flags().StringP("topic", "t", "", "Kafka topic")
	produceCmd.Flags().StringToString("headers", map[string]string{}, "Kafka message headers")
	produceCmd.MarkFlagRequired("brokers")
	produceCmd.MarkFlagRequired("proto")
	produceCmd.MarkFlagRequired("name")
	produceCmd.MarkFlagRequired("topic")
}

func parseProducerCommands(cmd *cobra.Command) (config.ProducerCommandFlags, error) {
	flags := cmd.Flags()

	brokers, err := flags.GetStringSlice("brokers")
	if err != nil {
		return config.ProducerCommandFlags{}, err
	}

	protoPath, err := flags.GetString("proto")
	if err != nil {
		return config.ProducerCommandFlags{}, err
	}

	protoMessageName, err := flags.GetString("name")
	if err != nil {
		return config.ProducerCommandFlags{}, err
	}

	topic, err := flags.GetString("topic")
	if err != nil {
		return config.ProducerCommandFlags{}, err
	}

	headers, err := flags.GetStringToString("headers")
	if err != nil {
		return config.ProducerCommandFlags{}, err
	}

	return config.ProducerCommandFlags{
		Brokers:          brokers,
		ProtoPath:        protoPath,
		ProtoMessageName: protoMessageName,
		Topic:            topic,
		Headers:          headers,
	}, nil
}

func run(cmd *cobra.Command, args []string) {
	commands, err := parseProducerCommands(cmd)
	if err != nil {
		panic(err)
	}

	cli, err := producer.NewCli(config.Producer{ProducerCommandFlags: commands})
	if err != nil {
		panic(err)
	}

	if err := cli.Start(); err != nil {
		panic(err)
	}
}
