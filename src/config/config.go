package config

type ProducerCommandFlags struct {
	Brokers          []string
	ProtoPath        string
	ProtoMessageName string
	Topic            string
	Headers          map[string]string
}

type Producer struct {
	ProducerCommandFlags
}
