package producer

import (
	"bufio"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/golang/protobuf/jsonpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"kafka-protobuf-cli/src/config"
	"kafka-protobuf-cli/src/protobuf"
	"os"
)

type kafkaProducer interface {
	Send(topic string, payload []byte, headers []sarama.RecordHeader) error
}

type Cli struct {
	kafkaProducer     kafkaProducer
	messageDescriptor protoreflect.MessageDescriptor
	topic             string
	headers           []sarama.RecordHeader
}

func NewCli(cfg config.Producer) (*Cli, error) {
	producer, err := NewKafkaProducer(cfg.Brokers)
	if err != nil {
		return nil, err
	}

	var recordHeaders []sarama.RecordHeader
	for headerKey, headerValue := range cfg.Headers {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{
			Key:   []byte(headerKey),
			Value: []byte(headerValue),
		})
	}

	messageDescriptor, err := protobuf.UnmarshalMessageFromProto(cfg.ProtoPath, cfg.ProtoMessageName)
	if err != nil {
		return nil, err
	}

	return &Cli{
		kafkaProducer:     producer,
		messageDescriptor: messageDescriptor,
		topic:             cfg.Topic,
		headers:           recordHeaders,
	}, nil
}

func (c *Cli) Start() error {
	scanner := bufio.NewScanner(os.Stdin)
	promptInput()
	for scanner.Scan() {
		err := c.processInput(scanner.Text())
		if err != nil {
			return err
		}
		promptInput()
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	return nil
}

func promptInput() {
	fmt.Print("> ")
}

func (c *Cli) processInput(input string) error {
	message := dynamicpb.NewMessage(c.messageDescriptor)
	if err := jsonpb.UnmarshalString(input, message); err != nil {
		return err
	}

	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return c.kafkaProducer.Send(c.topic, bytes, c.headers)
}
