package rabbitmq

import "time"

// awaitBroker runs a call that waits on the broker with no deadline of its own
// (a synchronous AMQP RPC, or a socket write to a broker that has stopped
// reading) and bounds it with a timer. On timeout the only way to make the
// pending call return is to sever the connection that owns the channel, which
// errors every call parked on it within the adapter's close deadline. The
// caller's sentinel is returned so monitors can count the event.
//
// The wait after the sever relies on an adapter invariant: a closed connection
// has no pending calls. amqp091 upholds it (connection shutdown closes every
// channel's error stream, which fails a parked RPC, and closes the socket,
// which fails a stalled write), and a sever that finds the connection already
// closed is a no-op precisely because that shutdown has already run. The
// second timer is the guard for an adapter that does not: it abandons the
// call's goroutine rather than park the caller forever. Because completed is
// buffered, a call that returns late does not leak.
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
		timer.Reset(timeout) // safe: the timer fired and its channel was drained
		select {
		case <-completed:
		case <-timer.C:
			logger.Printf("[WARN] AMQP %s still pending [%s] after the connection was severed; abandoning it.", operation, timeout)
		}
		return timeoutErr
	}
}
