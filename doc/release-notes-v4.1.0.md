# Release notes: v4.1.0

This release puts a time limit on every wait in the outbox publish path, makes
the consumer runtime observable, and fixes a race. It is a minor version. It
adds four options and changes no existing exported interface. A service can
upgrade without code changes. See the proposal in
`doc/work-sessions/2026/` for the full background.

## Why

On 2026-09-10 a consumer stopped for hours and logged nothing. A queue bound to
a fanout exchange was down. The broker held the transaction commit that routed
to it. The RabbitMQ writer waited for the commit with no time limit. The outbox
channel filled. Each handler committed its SQL work and then waited on the full
channel with no time limit. No handler returned, so no handler acknowledged its
deliveries. The process could not shut down, because the startup read also
waited on the full channel.

This release bounds each of those waits.

## New options and defaults

| Package    | Option                            | Default        |
|------------|-----------------------------------|----------------|
| `rabbitmq` | `Options.CommitTimeout`           | 30 seconds     |
| `sqlmq`    | `Options.HandoffTimeout`          | 10 seconds     |
| `sqlmq`    | `Options.DeferredHandoffCapacity` | 8192 messages  |

Each option replaces a zero or negative value with its default. A computed or
misparsed value cannot disable a bound.

```go
rabbitmq.New(rabbitmq.Options.CommitTimeout(30 * time.Second))

sqlmq.New(transport,
    sqlmq.Options.HandoffTimeout(10*time.Second),
    sqlmq.Options.DeferredHandoffCapacity(8192),
)
```

## `status`: the probe publishes inside a transaction

The probe used a plain publish. `basic.publish` is asynchronous, so a
channel-level fault (no write permission on the exchange, or a probe topic
whose exchange does not exist) arrived as a channel close *after* the probe
had returned success. The next probe failed with a generic closed-channel
error inside the tolerance window, the one after that reconnected and
succeeded, and the window reset every other call. The v4.0.0 promise that a
denied permission bypasses the window held only for connection-level faults.

The probe now opens a transactional writer, publishes, and commits.
`tx.commit` is synchronous: the broker either answers or closes the channel
with the reason, inside the same probe. A 403 or 404 on the probe topic is
now a definitive error, reported at once. The commit is bounded by
`rabbitmq.Options.CommitTimeout`, so the probe also detects the stall the
2026-09-10 incident produced when it lands on the probe topic. The rabbitmq
writer panics on a 404 at commit when `PanicOnTopologyError` is on; the
probe converts that panic into the definitive error instead of crashing the
service.

A successful probe now means the broker accepted the publish and answered
the commit. It still does not prove the message was routed anywhere.

## `rabbitmq`: closes initiated by the broker or network are reported

The connection registered for blocked notifications but never for close
notifications. A heartbeat timeout, a `CONNECTION_FORCED` from a node
shutdown, the 30-minute consumer acknowledgement timeout (`406`), or a
deleted queue surfaced only as a bare `EOF` on the next read, the reason was
lost, and `ConnectionClosed` never fired, so an open-minus-closed gauge
drifted upward forever.

The connection now logs
`[WARN] AMQP connection closed by the broker or network [...]`, fires
`ConnectionClosed` exactly once, and reports `Closed()` as true. A stream
whose channel the broker closed returns the broker's error from `Read`
instead of `EOF`; a consumer the broker cancelled returns an error naming the
consumer that wraps `io.EOF`. When a connection closes while the broker has
it blocked, the monitor receives `ConnectionUnblocked`, so a blocked gauge
does not stick at 1 after a sever.

## `rabbitmq`: closed connections are no longer retained by the connector

The connector kept every connection it ever opened in a list that only its
own `Close` emptied. Every reconnect during an outage, every commit-timeout
sever, every failed status probe, and every `transactional` batch (which
connects fresh) leaked a closed connection and its buffers for the life of
the process. A connection now removes itself from the list when it closes,
whether by its owner, by a sever, or by the broker.

## `rabbitmq`: message TTL is now sent in milliseconds (behavior change)

`Dispatch.Expiration` was rendered as whole seconds. The broker interprets the
AMQP expiration property as milliseconds, so a one-hour TTL expired after 3.6
seconds and any value below one second expired after one millisecond. Expired
messages vanish with no error, so this went unnoticed since the original
writer. The writer now sends `Expiration.Milliseconds()`, with a floor of 1.

A service that tuned its TTL against the old scale will see messages live
1000 times longer than before. Review every `Dispatch.Expiration` value when
you upgrade. A service that never sets `Expiration` is unaffected.

## `rabbitmq`: bounded transaction commit and rollback

`CommitWriter.Commit` and `CommitWriter.Rollback` now wait at most
`CommitTimeout` for the broker to answer. When the limit passes, the writer
does three things:

1. It logs `[WARN] AMQP transaction commit did not complete within [30s];
   severing the connection.`
2. It closes the connection that owns the channel. This is the only way to
   make the pending AMQP call return. The close uses the existing 5-second
   socket deadline.
3. It returns `rabbitmq.ErrCommitTimeout`. The `TransactionCommitted(err)` or
   `TransactionRolledBack(err)` monitor callback receives the same error.

The existing `[WARN] Unable to commit channel transaction [...]` line also
appears, because the timeout is a commit error like any other. The
`batch.Writer` and the `transactional` handler already reconnect after a
commit error. No new code is necessary in a service.

### Every broker wait is now bounded

`CommitTimeout` bounds more than the commit. The same timer-and-sever pattern
now covers:

- **Publish.** `Write` blocked in the socket write once a broker under a
  resource alarm stopped reading. It now times out, severs, and returns
  `ErrPublishTimeout` with zero written, so the caller retries the batch.
- **Acknowledge.** `Stream.Acknowledge` blocked the same way and ignored its
  context. It now returns `ErrAcknowledgeTimeout` after severing.
- **Channel close.** `Reader.Close` and `Writer.Close` are synchronous RPCs
  that ran before the bounded connection close and could hang a shutdown on a
  stuck channel. They now return `ErrCloseTimeout` after severing. Consumer
  cancel uses `noWait`, so `Stream.Close` never waits on the broker.
- **Connect.** The TLS handshake and the AMQP handshake set no deadline, so a
  peer that accepted TCP and then hung (an auth backend that never answers)
  parked every reconnect loop and the status probe forever. Both now run
  under the caller's deadline, or under `CommitTimeout` when the caller set
  none, and a failed AMQP handshake closes the socket instead of leaking it.

### Effect on shared connections

The sever closes every channel on the connection. In this library each
`CommitWriter` lives on a connection that the caller opened for it, so the
sever only affects the writer that is already stuck. A service that shares one
connection between a consumer and a transactional writer loses the consumer
stream when a commit times out. The stream reconnects on its own. If your
service cannot tolerate that reconnect, give the writer its own connection.

## `sqlmq`: the handoff always returns success after the SQL commit

`Commit` on the outbox writer stores the messages, commits the SQL
transaction, and then hands the messages to the dispatch processor. Once the
SQL transaction commits, the rows are durable. From that point `Commit`
returns `nil` on every path. This is a behavior change for one path:

- **Before:** when the context ended during the handoff, `Commit` returned
  the context error. The `transactional` handler panicked, the `retry` handler
  ran the batch again, and the handler's side effects happened twice.
- **Now:** `Commit` logs `[INFO] Context ended during handoff; [N] committed
  message(s) remain in durable storage for the next startup.` and returns
  `nil`. The caller acknowledges the delivery. The startup read publishes the
  rows when the process returns.

## `sqlmq`: deferred handoff on timeout

The handoff now waits at most `HandoffTimeout` for the dispatch processor to
accept the messages. When the limit passes, the messages that were not
accepted move to a background goroutine. That goroutine keeps sending them
into the outbox channel until it finishes or the context ends. `Commit` logs
`[WARN] Committed [N] message(s) to durable storage, but the dispatch
processor did not accept [M] of them within [10s]. The handoff continues in
the background.` and returns `nil`.

The background handoff runs on the process lifetime given to
`sqlmq.Options.Context`, not on the caller's context. A request-scoped caller
(an HTTP handler, a job with a timeout) can return and cancel its context
without stranding its committed rows until the next restart. The caller's
context bounds only the initial wait. When the dispatch processor stops, it
ends every background handoff; their rows stay durable for the next startup.

The messages stay in the memory of the same process. No other instance can see
them, so no instance publishes them twice. This release adds no periodic table
scan, because many services share one outbox table across instances.

### The deferred cap

`DeferredHandoffCapacity` limits the number of messages that background
handoffs hold at one time. When a new deferral would exceed the cap, `Commit`
does not defer. It logs `[WARN] Deferred handoff capacity [8192] reached;
waiting for the dispatch processor to accept [M] message(s).` and waits for
the channel or for shutdown. This is deliberate back-pressure. Memory stays
bounded, and the stall is already loud in the log.

Set `DeferredHandoffCapacity(1)` to disable deferral and apply back-pressure at
once. Raise the value to ride out longer broker outages without back-pressure.

A crash loses deferred messages from memory only. The startup read publishes
them from the table. This is the same guarantee the outbox gives every message
between its SQL commit and its broker confirm.

## `sqlmq`: publish failures and short confirms are logged

The dispatch processor retried a failed publish every `RetryTimeout` without
saying so. A permanent error, such as a dispatch with an empty topic, stalled
the whole outbox in silence. It now logs
`[WARN] Unable to publish [N] message(s) to the transport [...]; retrying in [5s].`
on every attempt.

`Confirm` now reports how many rows it updated, and `MessageConfirmed`
receives that number rather than the batch size. When fewer rows than
published are confirmed, the processor logs a `WARN` naming both counts. That
happens legitimately when another instance's startup read published the same
rows first, and it happens when `AutoincrementStride` does not match the
server's `auto_increment_increment`, in which case the MessageIDs are wrong
and the log line is the only signal.

## `sqlmq`: the outbox channel is no longer closed at shutdown

The dispatch processor used to close the outbox channel when `Listen`
returned. Handlers that had just committed SQL, and the new deferred handoff
goroutines, could still be sending on it. A send on a closed channel panics
even inside a `select`. A handler panicking after its SQL commit was retried
by the `retry` handler, and its side effects ran twice. The channel now stays
open. Nothing but the processor ever receives from it, so nothing is lost.

## `sqlmq`: shutdown no longer hangs on a full channel

The startup read sends every undispatched row into the outbox channel. Before
this release that send had no exit for shutdown. If the channel was full when
`Close` ran, `Listen` never returned and the host's shutdown hung. The send
now stops when the context ends. The rows stay durable for the next startup.

The startup read also reports what it found: `[INFO] Startup recovery found
[N] undispatched message(s) in durable storage.` The line does not appear when
the table holds no undispatched rows.

## `retry`: exponential backoff no longer overflows

With `MaxBackoff` set, the delay was computed as `Backoff << attempt`. Five
seconds shifted 31 times overflows a 64-bit duration, so attempt 31 produced a
negative delay (no sleep) and attempts 62 and up produced zero. A poison batch
that had been retrying at the cap for about two and a half hours flipped into
a zero-delay loop that opened a transaction and logged a stack trace on every
iteration. The delay now stays at `MaxBackoff` once the shift would overflow.

## `streaming`: new monitor

`streaming.Options.Monitor` is new. The consumer runtime had no monitor at
all, so there was no way to graph consumer throughput, batch latency, or
reconnect churn per queue. The interface has five methods:

```go
StreamOpened(streamName string, err error)
StreamClosed(streamName string)
BatchHandled(streamName string, count int, duration time.Duration)
BatchAcknowledged(streamName string, count int, err error)
ShutdownForced(streamName string)
```

Every callback carries the queue name from `NewSubscription`, so one
implementation serves every subscription in a process and labels its metrics
per stream. The default is a no-op. Because the interface is new, adding it
breaks nothing. `BatchHandled` and `BatchAcknowledged` fire once per batch on
the hot path; keep implementations cheap. `BatchHandled` measures the whole
`Handle` call, so it includes the attempts an inner `retry` handler makes. A
service that wraps its handler to time each attempt can keep that wrapper; the
two measure different things. The README's "Consuming" and "Monitoring and
alerting" sections describe each callback and show a per-stream
implementation for a metrics library with fixed labels.

## `streaming`: the logger is now used

`streaming.Options.Logger` existed before this release but nothing read it.
The consumer runtime swallowed every error. A missing queue, a refused
topology, an unreachable broker, or a failing acknowledgement all produced a
process that ran and consumed nothing, in silence. The runtime now logs at
each of those points, and at each reconnect and each forced shutdown. Pass a
logger. The lines and their meanings are in the table below and in the
README's "Consuming" section.

## `streaming`: an escaped handler panic no longer hangs the worker

When a panic escaped the outermost handler, the worker's `Listen` unwound
into a deferred wait for its reader goroutine. The reader was parked in
`Stream.Read` on a context that a graceful shutdown never cancels, so the
wait never ended, the runtime never printed the panic, and the dead worker
kept absorbing deliveries into a buffer nobody drained. Each worker now reads
on its own child context, cancelled before the wait. The panic is logged as
`[ERROR] Handler on stream [queue] panicked [...]; the worker is exiting.` and
then propagates, which ends the process. The broker redelivers after restart.

## `streaming`: one failing subscription no longer tears down the others

All subscriptions in a consumer share one connection. The subscriber closed
that connection on every exit, including a missing queue or a refused
binding on one subscription. Every sibling lost its stream, reconnected after
`ReconnectDelay`, and abandoned its in-flight batches, so a single bad queue
became a process-wide reconnect storm with duplicates on the healthy queues.

The subscriber now closes the shared connection only when the connection
itself is unusable: when opening a channel on it fails, or when the connection
reports that it is already closed. A failure to open a stream, or a stream
that ends, leaves the connection up for the other subscriptions. The
`rabbitmq` connection exposes `Closed()`, and the pool replaces a cached
connection that reports closed, so a connection severed by a commit timeout
is not handed to the next subscriber.

## `streaming`: connection pool race fixed

`Dispose` on the internal connection pool unlocked its mutex immediately
instead of deferring the unlock, so the write that clears the cached
connection ran unguarded and raced with `Active`. The unlock is now deferred.
A test runs both concurrently under the race detector.

## New log lines

| Level  | Line                                                                                                                                                        |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `WARN` | `AMQP transaction commit did not complete within [30s]; severing the connection.` (also `rollback`)                                                         |
| `WARN` | `Unable to commit channel transaction [...]` (existing line, now also for timeouts)                                                                         |
| `WARN` | `Committed [N] message(s) to durable storage, but the dispatch processor did not accept [M] of them within [10s]. The handoff continues in the background.` |
| `WARN` | `Deferred handoff capacity [8192] reached; waiting for the dispatch processor to accept [M] message(s).`                                                    |
| `INFO` | `Context ended during handoff; [M] committed message(s) remain in durable storage for the next startup.`                                                    |
| `INFO` | `Startup recovery found [N] undispatched message(s) in durable storage.`                                                                                    |
| `WARN` | `Unable to open connection for stream [queue] [...]` / `Unable to open reader for stream [queue] [...]` / `Unable to open stream [queue] [...]`              |
| `WARN` | `Unable to acknowledge [N] delivery(ies) from stream [queue] [...]; the broker will redeliver them.`                                                        |
| `WARN` | `Workers on stream [queue] did not conclude within [5s] of shutdown; abandoning in-flight deliveries.`                                                      |
| `INFO` | `Stream [queue] ended [...]`                                                                                                                                |
| `INFO` | `Subscription to stream [queue] concluded; reconnecting in [5s].`                                                                                           |

No monitor interface changed. The `rabbitmq` monitor receives
`ErrCommitTimeout` through the existing `TransactionCommitted` and
`TransactionRolledBack` callbacks. Count timeouts with `errors.Is`. The `sqlmq`
handoff and recovery events are log-only in this release. The `streaming`
package gains a monitor (below), which is additive because none existed.

## What a stall looks like now

With the defaults, a stuck publisher produces one `WARN` cycle about every 40
seconds: a 30-second commit wait, a 5-second connection close, and a 5-second
retry sleep. Handlers keep acknowledging their deliveries, up to the deferred
cap. When the broker recovers, the deferred handoffs finish and the publisher
resumes without a restart.

## Monitoring and alerting

The bounds in this release turn a silent stall into a loud one. They do not
fix the cause. Wire the monitor callbacks and the log lines above to metrics
and alerts. The README's [Monitoring and alerting](../README.md#monitoring-and-alerting)
section lists the metrics to emit, the patterns to page on, and a per-service
checklist. At a minimum: count `ErrCommitTimeout` separately from other commit
errors, graph stored minus confirmed outbox messages, and page on the
blocked-connection gauge and on the deferred-capacity `WARN` line.
