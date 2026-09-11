# Release notes: v4.1.0

This release puts a time limit on every wait in the outbox publish path. It is
a minor version. It adds three options and changes no exported interface. A
service can upgrade without code changes. See the proposal in
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

## `sqlmq`: shutdown no longer hangs on a full channel

The startup read sends every undispatched row into the outbox channel. Before
this release that send had no exit for shutdown. If the channel was full when
`Close` ran, `Listen` never returned and the host's shutdown hung. The send
now stops when the context ends. The rows stay durable for the next startup.

The startup read also reports what it found: `[INFO] Startup recovery found
[N] undispatched message(s) in durable storage.` The line does not appear when
the table holds no undispatched rows.

## New log lines

| Level  | Line                                                                                                                                                        |
|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `WARN` | `AMQP transaction commit did not complete within [30s]; severing the connection.` (also `rollback`)                                                         |
| `WARN` | `Unable to commit channel transaction [...]` (existing line, now also for timeouts)                                                                         |
| `WARN` | `Committed [N] message(s) to durable storage, but the dispatch processor did not accept [M] of them within [10s]. The handoff continues in the background.` |
| `WARN` | `Deferred handoff capacity [8192] reached; waiting for the dispatch processor to accept [M] message(s).`                                                    |
| `INFO` | `Context ended during handoff; [M] committed message(s) remain in durable storage for the next startup.`                                                    |
| `INFO` | `Startup recovery found [N] undispatched message(s) in durable storage.`                                                                                    |

No monitor interface changed. The `rabbitmq` monitor receives
`ErrCommitTimeout` through the existing `TransactionCommitted` and
`TransactionRolledBack` callbacks. Count timeouts with `errors.Is`. The `sqlmq`
handoff and recovery events are log-only in this release.

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
