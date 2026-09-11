#### SMARTY DISCLAIMER: Subject to the terms of the associated license agreement, this software is freely available for your use. This software is FREE, AS IN PUPPIES, and is a gift. Enjoy your new responsibility. This means that while we may consider enhancement requests, we may or may not choose to entertain requests at our sole and absolute discretion.

# messaging

[![Build](https://github.com/smarty/messaging/actions/workflows/build.yml/badge.svg)](https://github.com/smarty/messaging/actions/workflows/build.yml)

`github.com/smarty/messaging/v4` is a small set of Go packages for publishing and consuming messages
through RabbitMQ with a transactional outbox, at-least-once delivery, and a time limit on every wait.
A root package defines transport-neutral contracts. The other packages implement a transport, decorate
a transport, or consume one, and they compose in a fixed order.

```sh
go get github.com/smarty/messaging/v4
```

Requires Go 1.25 or later and a RabbitMQ broker. The outbox requires a MySQL-compatible database.

## Contents

- [Concepts](#concepts)
- [Packages](#packages)
- [Quick start](#quick-start)
- [The outbox](#the-outbox)
- [Consuming](#consuming)
- [Serialization](#serialization)
- [Health checks](#health-checks)
- [Bounded waits](#bounded-waits)
- [Monitoring and alerting](#monitoring-and-alerting)
- [Shutdown](#shutdown)
- [Development](#development)
- [Versioning and release notes](#versioning-and-release-notes)

## Concepts

The root package, `messaging`, declares the vocabulary. Every other package speaks it.

| Type           | Meaning                                                                                              |
|----------------|------------------------------------------------------------------------------------------------------|
| `Connector`    | Opens a `Connection`. Also an `io.Closer`.                                                           |
| `Connection`   | Produces a `Reader`, a `Writer`, or a `CommitWriter`.                                                |
| `Reader`       | Opens a `Stream` from a `StreamConfig` (queue name, topics, buffer sizes, topology flags).           |
| `Stream`       | `Read` one `Delivery` at a time; `Acknowledge` one or many.                                          |
| `Writer`       | `Write` one or many `Dispatch` values. A `CommitWriter` adds `Commit` and `Rollback`.                |
| `Dispatch`     | An outbound message: topic, partition key, type, content type, payload, headers, and a `Message` value. |
| `Delivery`     | An inbound message: the same fields plus delivery and source identifiers and the raw upstream object. |
| `Handler`      | `Handle(ctx, messages ...any)`. It returns nothing. Failure is a panic (see [Consuming](#consuming)). |
| `ListenCloser` | Something with a blocking `Listen()` and a `Close()`. Background processors implement it.            |

## Packages

| Package                  | Role                                                                                                                                        |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `rabbitmq`               | The transport. A `Connector` over [`amqp091-go`](https://github.com/rabbitmq/amqp091-go). Heartbeats, blocked-connection notifications, bounded transaction commits. |
| `serialization`          | A decorator. Encodes `Dispatch.Message` into `Payload` on the way out and decodes `Delivery.Payload` into `Message` on the way in. JSON by default. |
| `sqlmq`                  | A decorator and the outbox. Its `CommitWriter` stores messages in a SQL table inside your transaction. A background processor publishes them later. |
| `batch`                  | A `Writer` that does connect, write, and commit as one publish, and redials after any error. `sqlmq` uses it to publish.                    |
| `streaming`              | The consumer runtime. Opens a stream per subscription, runs one goroutine per handler, batches deliveries, calls handlers, acknowledges.       |
| `handlers/transactional` | Wraps a handler in a `CommitWriter`. With `sqlmq`, the handler receives the live `*sql.Tx` and a `Writer` bound to the same transaction.       |
| `handlers/retry`         | Recovers a panic from the inner handler and runs the batch again with backoff and jitter.                                                   |
| `handlers/sqltx`         | Like `transactional`, for a plain `*sql.DB` with no messaging.                                                                              |
| `handlers/multi`         | Fans one batch to several handlers in order.                                                                                                |
| `status`                 | A health probe for `/status` endpoints. Tolerates brief broker failures. Reports credential and permission faults at once.                  |

`rabbitmq/adapter` and `sqlmq/adapter` are thin interfaces over the AMQP client and `database/sql`.
They exist so the packages above them can be tested with fakes. You will only touch them in tests.

## Quick start

The packages compose in one order. The types enforce most of it.

```go
package main

import (
	"context"
	"database/sql"
	"reflect"
	"time"

	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/handlers/retry"
	"github.com/smarty/messaging/v4/handlers/transactional"
	"github.com/smarty/messaging/v4/rabbitmq"
	"github.com/smarty/messaging/v4/serialization"
	"github.com/smarty/messaging/v4/sqlmq"
	"github.com/smarty/messaging/v4/streaming"
)

type OrderPlaced struct{ OrderID uint64 }
type OrderShipped struct{ OrderID uint64 }

func main() {
	db, _ := sql.Open("mysql", "user:pass@tcp(127.0.0.1:3306)/app?parseTime=true")

	// 1. The transport.
	transport := rabbitmq.New(
		rabbitmq.Options.Address("amqp://guest:guest@127.0.0.1:5672/"),
		rabbitmq.Options.Monitor(myRabbitMonitor{}),
	)

	// 2. Serialization wraps the transport, so the outbox stores serialized rows.
	encoded := serialization.New(transport,
		serialization.Options.ReadTypes(map[string]reflect.Type{
			"order-placed": reflect.TypeOf(OrderPlaced{}),
		}),
		serialization.Options.WriteTypes(map[reflect.Type]string{
			reflect.TypeOf(OrderShipped{}): "order-shipped",
		}),
	)

	// 3. The outbox wraps the encoded transport. It returns a Connector for
	//    handlers to write through and a processor that publishes in the background.
	outbox, dispatcher := sqlmq.New(encoded,
		sqlmq.Options.StorageHandle(db),
		sqlmq.Options.Monitor(myOutboxMonitor{}),
	)

	// 4. A handler runs inside one SQL transaction. Its own writes and its
	//    outgoing messages commit together, or not at all.
	handler := retry.New(
		transactional.New(outbox, func(state transactional.State) messaging.Handler {
			return &shipOrders{tx: state.Tx, writer: state.Writer}
		}),
		retry.Options.Backoff(time.Second),
		retry.Options.MaxBackoff(time.Minute),
	)

	// 5. The consumer reads decoded deliveries and feeds the handler.
	consumer := streaming.New(encoded,
		streaming.Options.Subscriptions(
			streaming.NewSubscription("orders",
				streaming.SubscriptionOptions.Topics("order-placed"),
				streaming.SubscriptionOptions.AddWorkers(handler, handler, handler),
				streaming.SubscriptionOptions.BatchCapacity(64),
			),
		),
	)

	go dispatcher.Listen()
	go consumer.Listen()
	// ... on shutdown:
	_ = consumer.Close()
	_ = dispatcher.Close()
}

type shipOrders struct {
	tx     *sql.Tx
	writer messaging.Writer
}

func (this *shipOrders) Handle(ctx context.Context, messages ...any) {
	for _, message := range messages {
		placed := message.(OrderPlaced)
		if _, err := this.tx.ExecContext(ctx, "UPDATE orders SET shipped = 1 WHERE id = ?", placed.OrderID); err != nil {
			panic(err) // retry recovers this and runs the batch again
		}
		_, _ = this.writer.Write(ctx, messaging.Dispatch{Message: OrderShipped{OrderID: placed.OrderID}})
	}
}
```

Why this order:

- `serialization.New` must wrap the transport **before** `sqlmq.New` sees it, so the outbox stores rows
  that are already encoded and the processor can publish them without a type registry.
- `transactional.New` takes the **outbox** connector. That is what puts the `*sql.Tx` into the handler's hands.
- `retry.New` wraps `transactional.New`, never the reverse. `transactional` reports failure by panicking,
  and only an outer `retry` can recover it.
- `streaming.New` takes the **encoded** connector, because decoding happens inside `Stream.Read`.

## The outbox

`sqlmq` implements the transactional outbox pattern. A handler's own database writes and the messages it
publishes share one SQL transaction. Either both commit or neither does. Messages then reach the broker
at least once, possibly more than once after a crash. Consumers must be idempotent.

Create the table once per database. The schema is in `sqlmq/_schema_mysql.sql`:

```sql
CREATE TABLE Messages (
    id         bigint unsigned AUTO_INCREMENT NOT NULL,
    dispatched datetime(3)                        NULL,
    type       varchar(256)                   NOT NULL,
    payload    mediumblob                     NOT NULL,
    PRIMARY KEY (id)
);
CREATE UNIQUE INDEX ix_messages_dispatched ON Messages (dispatched, id);
```

A `NULL` in `dispatched` means the row has not reached the broker. The flow is:

1. The handler writes dispatches to the `sqlmq` `CommitWriter`. They are buffered in memory.
2. `Commit` inserts the rows in the handler's transaction and commits SQL. The rows are now durable.
3. `Commit` hands the dispatches to an in-memory channel and returns success. After the SQL commit,
   `Commit` **always** returns `nil`, because an error would make the caller run the batch again.
4. The dispatch processor drains the channel, publishes through the wrapped transport in one AMQP
   transaction, and sets `dispatched`. On any error it waits `RetryTimeout` and tries the same batch again.
5. At startup the processor loads every row with a `NULL` `dispatched` and publishes it. There is no
   periodic table scan, because many service instances share one table and a scan would republish rows
   another instance holds in flight.

Options you are most likely to set:

| Option                                    | Default   | Purpose                                                                 |
|-------------------------------------------|-----------|-------------------------------------------------------------------------|
| `Options.StorageHandle(*sql.DB)`          | required  | The database. `Options.DataSource(driver, dsn)` opens one for you.      |
| `Options.ChannelBufferCapacity(int)`      | 1024      | Size of the in-memory channel between handlers and the processor.       |
| `Options.RetryTimeout(time.Duration)`     | 5 s       | Sleep between publish attempts after an error.                          |
| `Options.IsolationLevel(sql.IsolationLevel)` | ReadCommitted | Isolation for the outbox transaction.                              |
| `Options.AutoincrementStride(uint8)`      | 1         | Gap between consecutive ids; used to assign `MessageID`s after a batch insert. Match your server's `auto_increment_increment`. |
| `Options.HandoffTimeout(time.Duration)`   | 10 s      | See [Bounded waits](#bounded-waits).                                    |
| `Options.DeferredHandoffCapacity(int)`    | 8192      | See [Bounded waits](#bounded-waits).                                    |
| `Options.Logger`, `Options.Monitor`       | no-op     | See [Monitoring and alerting](#monitoring-and-alerting).                |

## Consuming

`streaming.New` takes a connector and one or more subscriptions. Each subscription names a queue, the
topics (exchanges) to bind it to, and one handler per worker goroutine. Deliveries are read into a
buffer, gathered into batches opportunistically, handed to `Handler.Handle`, and acknowledged after
`Handle` returns.

`Handler.Handle` has no return value. **Failure is a panic.** `transactional` and `sqltx` panic when a
connection, transaction, or commit fails, and re-panic after rolling back. `retry` recovers the panic,
logs it, sleeps with exponential backoff and jitter, and runs the batch again. `streaming` acknowledges
only after `Handle` returns, so a batch that panics past `retry`'s `MaxAttempts` crashes the worker and
the broker redelivers it after reconnect. Design handlers to be idempotent.

Subscription options you are most likely to set:

| Option                                                | Default    | Purpose                                                                     |
|-------------------------------------------------------|------------|-----------------------------------------------------------------------------|
| `SubscriptionOptions.Name(string)`                    | empty      | Consumer or group name reported to the broker.                              |
| `SubscriptionOptions.Topics(...string)`               | none       | Exchanges to bind the queue to.                                             |
| `SubscriptionOptions.AddWorkers(...messaging.Handler)`| required   | One goroutine per handler. All workers share one stream.                    |
| `SubscriptionOptions.BufferCapacity(uint16)`          | 1          | Deliveries prefetched into local memory.                                    |
| `SubscriptionOptions.BatchCapacity(uint16)`           | 1          | Maximum deliveries per `Handle` call. Batches never wait to fill.           |
| `SubscriptionOptions.FullThrottle()`                  | off        | Sets both capacities to the maximum.                                        |
| `SubscriptionOptions.EstablishTopology(bool)`         | true       | Declare the queue and exchanges and bind them on connect.                   |
| `SubscriptionOptions.StreamReplication(bool)`         | false      | Use quorum queues.                                                          |
| `SubscriptionOptions.FullDeliveryToHandler(bool)`     | false      | Pass `messaging.Delivery` values instead of `Delivery.Message`.             |
| `SubscriptionOptions.ReconnectDelay(time.Duration)`   | 5 s        | Pause before reopening a stream after it ends.                              |
| `SubscriptionOptions.ShutdownStrategy(strategy, timeout)` | Drain, 5 s | See [Shutdown](#shutdown).                                              |

`retry` options: `Backoff` (5 s), `MaxBackoff` (0, which disables growth), `JitterFactor` (0 to 1),
`MaxAttempts` (effectively unlimited), `ImmediateRetry(values...)` for panic values that should not
sleep, `LogStackTrace` (on).

## Serialization

`serialization.New` wraps any connector. On write, it looks up the Go type of `Dispatch.Message` in
`WriteTypes`, serializes it, and fills `Payload`, `ContentType`, and `MessageType`. When `Topic` is empty
it also sets `Topic` to the message type name, so a dispatch needs only a `Message`. On read, it looks up
`Delivery.MessageType` in `ReadTypes`, picks a deserializer by `ContentType`, and fills `Delivery.Message`
with a value of that type.

The default serializer is JSON with content type `application/json`. It is also registered for the
empty content type, so payloads written before content types were set still decode. Add others with
`Options.Serializer` and `Options.AddDeserializer(value, contentTypes...)`.

Three consumer-side failure modes each have an `Ignore*` switch. When on, the library logs a `WARN`,
reports the error to the monitor, and delivers the message with a `nil` `Message`. When off, the error
ends the stream read and the subscription reconnects.

| Condition                       | Switch                                         | Default |
|---------------------------------|------------------------------------------------|---------|
| Type name not in `ReadTypes`    | `Options.IgnoreUnknownMessageTypes(bool)`      | off     |
| No deserializer for content type| `Options.IgnoreUnknownContentTypes(bool)`      | off     |
| Payload does not parse          | `Options.IgnoreDeserializationErrors(bool)`    | off     |

`Options.AllowedTypes(map[string]struct{})` filters by wire type name. A filtered message is
acknowledged without reaching the handler.

## Health checks

`status.New(status.Options.Connector(transport))` returns a `Checker` whose `Status(ctx) error` publishes
one empty message to a probe topic (`amq.direct` by default). Wire it to your `/status` endpoint.

The checker tolerates failures inside a window (`Options.FailureTolerance`, default 30 seconds). While
consecutive probes fail inside the window it logs a `WARN` and returns `nil`, so a broker failover does
not restart your service. Past the window it returns the error. AMQP `ACCESS_REFUSED` (403) and
`NOT_ALLOWED` (530) bypass the window, because a bad credential or missing vhost does not fix itself.
Size the window just above your longest routine broker event.

A restart is the intended reaction to a `/status` failure. It runs outbox recovery and drains the backlog.
Confirm your platform does that before you deploy.

## Bounded waits

Every wait in the publish path has a time limit. Each option replaces a zero or negative value with its
default, so a bound cannot be disabled by accident.

| Package    | Option                            | Default       | Bounds                                                                 |
|------------|-----------------------------------|---------------|------------------------------------------------------------------------|
| `rabbitmq` | `Options.CommitTimeout`           | 30 seconds    | The wait for the broker to answer a transaction commit or rollback.    |
| `rabbitmq` | `Options.Heartbeat`               | 10 seconds    | How long a dead socket goes unnoticed (about 1.5 times this value).   |
| `sqlmq`    | `Options.HandoffTimeout`          | 10 seconds    | The wait, after the SQL commit, to hand messages to the dispatcher.    |
| `sqlmq`    | `Options.DeferredHandoffCapacity` | 8192 messages | The messages that background handoffs may hold in memory at one time.  |

What happens at each bound:

- **Commit timeout.** The writer logs a `WARN`, closes the connection that owns the channel (the only way
  to make a pending AMQP call return), and returns `rabbitmq.ErrCommitTimeout`. The `batch.Writer` and
  the `transactional` handler reconnect on the next call. The close takes every channel on that
  connection with it, so give a transactional writer its own connection if a consumer shares one.
- **Handoff timeout.** The outbox `Commit` moves the messages the processor has not yet accepted to a
  background goroutine, logs a `WARN`, and returns success. The rows are already durable. Handlers keep
  acknowledging while the publisher is stalled.
- **Deferred capacity.** When background handoffs already hold this many messages, `Commit` does not
  defer. It logs a `WARN` and waits for the processor. This is deliberate back-pressure that keeps memory
  bounded. Set the capacity to 1 to apply back-pressure at once.

With the defaults, a stuck publisher produces one `WARN` cycle about every 40 seconds and recovers on its
own when the broker does. See `doc/release-notes-v4.1.0.md` for the incident that motivated this.

## Monitoring and alerting

The bounds above turn a silent stall into a loud one. They do not fix the cause. A down queue, a blocked
broker, or a bad binding still needs a person. The library reports each event through monitor callbacks
and log lines. Your service must wire those signals to metrics and alerts, or the stall stays invisible
to the on-call engineer until a consumer-lag graph shows it hours later.

### Wire the monitor callbacks to metrics

Implement `rabbitmq.Options.Monitor` and `sqlmq.Options.Monitor` in every service. Empty bodies satisfy
the contracts, but they discard the signal. At a minimum, emit these:

| Callback                                        | Metric                                      | Type    |
|-------------------------------------------------|---------------------------------------------|---------|
| `rabbitmq` `TransactionCommitted(err)`          | `amqp_commit_total{result}`                 | counter |
| `rabbitmq` `TransactionCommitted(err)`, timeout | `amqp_commit_timeout_total`                 | counter |
| `rabbitmq` `TransactionRolledBack(err)`         | `amqp_rollback_total{result}`               | counter |
| `rabbitmq` `ConnectionOpened(err)`              | `amqp_connection_open_total{result}`        | counter |
| `rabbitmq` `ConnectionClosed()`                 | `amqp_connection_close_total`               | counter |
| `rabbitmq` `ConnectionBlocked(reason)`          | `amqp_connection_blocked` (1 while blocked) | gauge   |
| `rabbitmq` `ConnectionUnblocked()`              | `amqp_connection_blocked` (back to 0)       | gauge   |
| `rabbitmq` `DispatchPublished()`                | `amqp_publish_total`                        | counter |
| `sqlmq` `MessageStored(count)`                  | `outbox_stored_total`                       | counter |
| `sqlmq` `MessagePublished(count)`               | `outbox_published_total`                    | counter |
| `sqlmq` `MessageConfirmed(count)`               | `outbox_confirmed_total`                    | counter |

Count a commit as a success when `err == nil`, as a timeout when
`errors.Is(err, rabbitmq.ErrCommitTimeout)`, and as a failure otherwise. Keep the timeout counter
separate, because a single callback cannot tell a timeout from a broker refusal.

The `retry`, `transactional`, `sqltx`, and `serialization` packages have their own small monitors
(`HandleAttempted`, `TransactionStarted/Committed/RolledBack`, `MessageEncoded/Decoded`). Count
`HandleAttempted` with a non-nil result: a rising rate means handlers are failing and retrying.

### The patterns an alerting system must respond to

**Commit timeouts.** One `amqp_commit_timeout_total` increment is a broker event worth a look. A rate of
one every 40 seconds for more than a few minutes is a stalled publisher. Page on this. The library
reconnects and retries on its own, but it cannot bring a queue back. The most common cause is a queue
bound to the target exchange that is down or has no live replica.

**Stored minus confirmed.** The difference between `outbox_stored_total` and `outbox_confirmed_total`
over a window is the number of committed messages the broker has not yet confirmed. In a healthy service
the two rates match within seconds. A gap that grows for more than one commit cycle means the publisher
is stalled. This is the single best health signal for the outbox, because it measures the outcome and
not the mechanism. Alert when the gap grows for longer than the handoff timeout plus the commit timeout.

**Deferred handoffs.** The outbox handoff events are log-only. Match and count the line
`The handoff continues in the background`. Each occurrence is one handler batch that waited longer than
`HandoffTimeout`. Alert on the first occurrence. Page when `Deferred handoff capacity [N] reached`
appears, because from that point handlers block, consumer lag grows, and the outage is visible to
customers.

**Blocked connection.** When `amqp_connection_blocked` is 1, the broker has a memory or disk alarm and has
stopped reading the socket. A small batch still reaches its commit, and the commit timeout fires. A large
batch stalls inside the socket write, before the commit, and no timeout fires. Page on this gauge
directly, so the alert does not depend on batch size. A restart does not help. The broker needs attention.

**Connection churn.** A commit timeout closes one connection and the writer opens another, so the open
and close counters rise together about once per commit cycle. On their own they are noisy during a
rolling broker restart. Combine them with the timeout counter to tell a sever from a normal reconnect.

**Startup recovery.** The line `Startup recovery found [N] undispatched message(s)` appears once per
start. A small `N` after a deploy is normal. A large or rising `N` across restarts means the service is
restarting faster than it can drain its backlog. Record `N` as a gauge and alert when it exceeds a few
channel capacities.

### What not to alert on

Do not page on a single `WARN` line or a single commit timeout. Broker failovers, rolling restarts, and
network blips produce one or two of each. The `status.Checker` tolerates probe failures inside its window
for the same reason. Alert on rates and on gaps that persist past one full cycle.

Do not treat `[INFO] Context ended during handoff` as a fault. It appears during a normal shutdown when
the publisher is behind. The rows are durable and the next start publishes them.

### Log lines worth a log-pipeline rule

| Level  | Line                                                                                                                                                        |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `WARN` | `AMQP transaction commit did not complete within [30s]; severing the connection.` (also `rollback`)                                                         |
| `WARN` | `Unable to commit channel transaction [...]`                                                                                                                |
| `WARN` | `AMQP connection blocked by broker (reason: ...)`                                                                                                           |
| `WARN` | `Committed [N] message(s) to durable storage, but the dispatch processor did not accept [M] of them within [10s]. The handoff continues in the background.` |
| `WARN` | `Deferred handoff capacity [8192] reached; waiting for the dispatch processor to accept [M] message(s).`                                                    |
| `INFO` | `Context ended during handoff; [M] committed message(s) remain in durable storage for the next startup.`                                                    |
| `INFO` | `Startup recovery found [N] undispatched message(s) in durable storage.`                                                                                    |

### A minimal checklist per service

1. Implement the `rabbitmq` and `sqlmq` monitor interfaces and emit the counters above.
2. Count `ErrCommitTimeout` separately from other commit errors.
3. Build the stored-minus-confirmed gap panel and alert on it.
4. Match and count the two outbox handoff `WARN` lines in your log pipeline.
5. Page on the blocked-connection gauge and on the deferred-capacity line.
6. Confirm `/status` fails for a stalled publisher in your platform, and that the platform's reaction
   (a restart) is what you want.

## Shutdown

Call `Close()` on the streaming consumer first, then on the outbox dispatcher.

The consumer uses two contexts. `Close()` cancels the **soft** context: the stream stops delivering new
messages and workers finish according to the subscription's `ShutdownStrategy`. If they have not
finished when the strategy's timeout passes, the **hard** context is cancelled and reads and
acknowledgements stop at once.

| Strategy                       | Behavior                                                             |
|--------------------------------|----------------------------------------------------------------------|
| `ShutdownStrategyDrain`        | Handle everything already buffered, then stop. Default, 5 s timeout. |
| `ShutdownStrategyCurrentBatch` | Finish and acknowledge the batch in progress, then stop.             |
| `ShutdownStrategyImmediate`    | Stop before the next batch. No timeout.                              |

The outbox dispatcher's `Close()` cancels its context. Handlers that are mid-handoff log an `INFO` line
and return success. The startup read stops. `Listen()` returns even when the channel is full. Rows that
did not reach the broker are published at the next start.

## Development

```sh
make test      # go mod tidy, go fmt, go test -timeout=1s -short -race -covermode=atomic ./...
make compile   # go build ./...
make build     # both; this is what CI runs
```

The suite runs under a one-second global timeout. Tests that wait use single-digit millisecond durations
against fakes that block until released. Tests use [`gunit`](https://github.com/smarty/gunit) fixtures
with `should` assertions. Each fixture implements the package's own interfaces as its fakes, so most
packages need no external services to test.

Design work for larger changes lives in `doc/work-sessions/`. Each is a self-contained HTML proposal
with an implementation checklist. `CLAUDE.md` describes the architecture and conventions for automated
contributors. See `CONTRIBUTING.md` before opening a pull request.

## Versioning and release notes

The module path is `github.com/smarty/messaging/v4`. Adding a method to any package's `monitor`
interface breaks every implementer and is a major-version change. Additive options and new sentinel
errors are minor versions.

| Release | Notes                                                                                              |
|---------|----------------------------------------------------------------------------------------------------|
| v4.1.0  | [`doc/release-notes-v4.1.0.md`](doc/release-notes-v4.1.0.md): bounded commit, deferred outbox handoff, shutdown-safe startup read. |
| v4.0.0  | [`doc/release-notes-v4.0.0.md`](doc/release-notes-v4.0.0.md): honest status checker, client heartbeat, blocked-connection notifications. Migration steps from v3. |

The `v4.0.0-alpha.*` tags belong to an abandoned 2021 experiment and are retracted in `go.mod`.

## License

See [`LICENSE.md`](LICENSE.md).
