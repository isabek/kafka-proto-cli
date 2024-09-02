package producer

import (
	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	client sarama.SyncProducer
}

func NewKafkaProducer(brokers []string) (KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	return KafkaProducer{client: producer}, err
}

func (p KafkaProducer) Send(topic string, data []byte, headers []sarama.RecordHeader) error {
	payload := sarama.ByteEncoder(data)
	message := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   payload,
		Headers: headers,
	}
	_, _, err := p.client.SendMessage(message)
	return err
}
