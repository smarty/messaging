package rabbitmq

import "time"

// awaitBroker runs a call that waits on the broker with no deadline of its own
// (a synchronous AMQP RPC, or a socket write to a broker that has stopped
// reading) and bounds it with a timer. On timeout the only way to make the
// pending call return is to sever the connection that owns the channel, which
// errors every call parked on it within the adapter's close deadline. The
// caller's sentinel is returned so monitors can count the event.
func awaitBroker(logger logger, operation string, timeout time.Duration, sever func() error, timeoutErr error, call func() error) error {
	completed := make(chan error, 1)
	go func() { completed <- call() }()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-completed:
		return err
	case <-timer.C:
		logger.Printf("[WARN] AMQP %s did not complete within [%s]; severing the connection.", operation, timeout)
		_ = sever()
		<-completed // bounded: the severed connection errors the pending call within the close deadline
		return timeoutErr
	}
}
