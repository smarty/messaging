package mq

import (
	"log"
	"strings"

	"github.com/streadway/amqp"
)

type Channel struct {
	inner *amqp.Channel
}

func newChannel(inner *amqp.Channel) *Channel {
	return &Channel{inner: inner}
}

func (this *Channel) ConfigureChannelBuffer(messageCount int) error {
	return this.inner.Qos(messageCount, 0, false)
}
func (this *Channel) ConfigureChannelAsTransactional() error {
	return this.inner.Tx()
}

func (this *Channel) DeclareExchange(name, kind string) error {
	return this.inner.ExchangeDeclare(name, kind, true, false, false, false, nil)
}
func (this *Channel) DeclareQueue(name string) error {
	_, err := this.inner.QueueDeclare(name, true, false, false, false, nil)
	return err
}
func (this *Channel) DeclareTransientQueue() (string, error) {
	if queue, err := this.inner.QueueDeclare("", false, true, false, false, nil); err != nil {
		return "", err
	} else {
		return queue.Name, nil
	}
}
func (this *Channel) BindExchangeToQueue(queue, exchange string) error {
	return this.inner.QueueBind(queue, "", exchange, false, nil)
}

func (this *Channel) Consume(queueName, consumerName string) (<-chan amqp.Delivery, error) {
	return this.inner.Consume(queueName, consumerName, false, false, true, false, nil)
}
func (this *Channel) ConsumeWithoutAcknowledgement(queueName, consumerName string) (<-chan amqp.Delivery, error) {
	return this.inner.Consume(queueName, consumerName, true, true, true, false, nil)
}
func (this *Channel) ExclusiveConsume(queueName, consumerName string) (<-chan amqp.Delivery, error) {
	return this.inner.Consume(queueName, consumerName, false, true, true, false, nil)
}
func (this *Channel) ExclusiveConsumeWithoutAcknowledgement(queueName, consumerName string) (<-chan amqp.Delivery, error) {
	return this.inner.Consume(queueName, consumerName, true, true, true, false, nil)
}
func (this *Channel) CancelConsumer(consumerName string) error {
	return this.inner.Cancel(consumerName, false)
}

func (this *Channel) AcknowledgeSingleMessage(deliveryTag uint64) error {
	return this.inner.Ack(deliveryTag, false)
}
func (this *Channel) AcknowledgeMultipleMessages(deliveryTag uint64) error {
	return this.inner.Ack(deliveryTag, true)
}

func (this *Channel) PublishMessage(destination string, message amqp.Publishing) error {
	if strings.HasPrefix(destination, "@") {
		return this.inner.Publish("", destination[1:], false, false, message)
	} else {
		return this.inner.Publish(destination, "", false, false, message)
	}
}

func (this *Channel) CommitTransaction() error {
	return this.inner.TxCommit()
}
func (this *Channel) RollbackTransaction() error {
	return this.inner.TxRollback()
}

func (this *Channel) Close() error {
	log.Println("[INFO] Closing AMQP channel.")
	return this.inner.Close()
}
