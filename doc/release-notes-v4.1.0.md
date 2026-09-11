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
