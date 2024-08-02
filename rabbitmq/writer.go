package rabbitmq

import (
	"context"
	"net/http"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/smarty/messaging/v3"
	"github.com/smarty/messaging/v3/rabbitmq/adapter"
)

type defaultWriter struct {
	inner         adapter.Channel
	topologyPanic bool
	now           func() time.Time
	logger        logger
	monitor       monitor
}

func newWriter(inner adapter.Channel, config configuration) messaging.CommitWriter {
	config.Logger.Printf("[INFO] Writer channel established on AMQP connection.")
	return defaultWriter{
		inner:         inner,
		topologyPanic: config.TopologyFailurePanic,
		now:           config.Now,
		logger:        config.Logger,
		monitor:       config.Monitor,
	}
}

func (this defaultWriter) Write(_ context.Context, messages ...messaging.Dispatch) (count int, err error) {
	now := this.now().UTC()

	for _, message := range messages {
		if len(message.Topic) == 0 {
			return count, messaging.ErrEmptyDispatchTopic
		}

		count++
		converted := toAMQPDispatch(message, now)

		partition := formatPartition(message.Partition)
		if err = this.inner.Publish(message.Topic, partition, converted); err != nil {
			this.logger.Printf("[WARN] Unable to write dispatch to underlying channel [%s].", err)
			return count - 1, err // writes are async, only channel unavailability causes errors here
		}

		this.monitor.DispatchPublished()
	}

	return count, nil
}
func formatPartition(value uint64) string {
	if value == 0 {
		return ""
	}

	return strconv.FormatUint(value, 10)
}
func toAMQPDispatch(dispatch messaging.Dispatch, now time.Time) amqp.Publishing {
	if dispatch.Timestamp.IsZero() {
		dispatch.Timestamp = now
	}

	return amqp.Publishing{
		AppId:           strconv.FormatUint(dispatch.SourceID, 10),
		MessageId:       strconv.FormatUint(dispatch.MessageID, 10),
		CorrelationId:   strconv.FormatUint(dispatch.CorrelationID, 10),
		Type:            dispatch.MessageType,
		ContentType:     dispatch.ContentType,
		ContentEncoding: dispatch.ContentEncoding,
		Timestamp:       dispatch.Timestamp,
		Expiration:      computeExpiration(dispatch.Expiration),
		DeliveryMode:    computePersistence(dispatch.Durable),
		Headers:         dispatch.Headers,
		Body:            dispatch.Payload,
	}
}
func computeExpiration(expiration time.Duration) string {
	if expiration == 0 {
		return ""
	} else if seconds := int(expiration.Seconds()); seconds <= 0 {
		return "1"
	} else {
		return strconv.FormatUint(uint64(seconds), 10)
	}
}
func computePersistence(durable bool) uint8 {
	if durable {
		return amqp.Persistent
	}

	return amqp.Transient
}

func (this defaultWriter) Commit() error {
	if err := this.inner.TxCommit(); err == nil {
		this.monitor.TransactionCommitted(nil)
		return nil
	} else {
		this.logger.Printf("[WARN] Unable to commit channel transaction [%s].", err)
		this.monitor.TransactionCommitted(err)
		return this.tryPanic(err)
	}
}
func (this defaultWriter) Rollback() error {
	if err := this.inner.TxRollback(); err == nil {
		this.monitor.TransactionRolledBack(nil)
		return nil
	} else {
		this.logger.Printf("[WARN] Unable to rollback channel transaction [%s].", err)
		this.monitor.TransactionRolledBack(err)
		return this.tryPanic(err)
	}
}
func (this defaultWriter) tryPanic(err error) error {
	if !this.topologyPanic {
		return err
	}

	if brokerError, ok := err.(*amqp.Error); ok && brokerError.Code == http.StatusNotFound {
		panic(err)
	}

	return err
}

func (this defaultWriter) Close() error {
	return this.inner.Close()
}
