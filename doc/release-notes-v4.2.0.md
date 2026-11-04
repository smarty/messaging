# Release notes: v4.2.0

This release reports three things the library knew but did not say: how long
each delivery waited, what startup recovery found, and when an outbox handoff
fell behind. It also lets a monitor recognize broker timeouts without importing
`rabbitmq`. It is a minor version. It changes no existing exported interface. A
service can upgrade without code changes.

## Why

A freshness objective ("messages are handled within 60 seconds of commit") needs
the age of each delivery. Deliveries already carried a timestamp, but nothing
reported it. Startup recovery counts and handoff deferrals reached operators only
as log lines, which cannot be graphed or alerted on.

A shared telemetry module for the -context services implements these monitors
without importing this library. It recognizes timeouts through the `net.Error`
convention, which the sentinels did not follow.

## Optional monitor extensions

Each is an exported interface. The library finds it with a type assertion on the
monitor given to `Options.Monitor`. A monitor without it works as before.

| Package     | Interface                | Method                            |
|-------------|--------------------------|-----------------------------------|
| `streaming` | `DeliveryAgeMonitor`     | `DeliveriesAged(stream, ages...)` |
| `sqlmq`     | `StartupRecoveryMonitor` | `StartupRecovered(count)`         |
| `sqlmq`     | `HandoffMonitor`         | `HandoffDeferred(count)`          |
| `sqlmq`     | `HandoffMonitor`         | `HandoffCapacityReached()`        |

`DeliveriesAged` fires once per batch, just before `Handle`. It carries the age of
each delivery handed to the handler, measured from `Delivery.Timestamp`. That is
the time `sqlmq` stored the message, or the time the transport published it.
Deliveries without a timestamp are skipped. The worker reuses one slice for
every batch, so the callback does not allocate. Do not retain the slice.

AMQP carries timestamps in whole seconds. An age can read up to a second high.
Use histogram buckets of a second or more. Ages compare two hosts' clocks, so a
skewed producer can make one negative.

`StartupRecovered` fires after each startup load that finds undispatched
messages. A load that fails part way is retried, so the counts of every call add
up to the total recovered. `HandoffDeferred` and `HandoffCapacityReached` fire at
the same points as the existing `[WARN]` log lines.

Pin each extension at compile time. A method whose signature drifts is never
called, and nothing says so:

```go
var (
    _ streaming.DeliveryAgeMonitor = (*myStreamingMonitor)(nil)
    _ sqlmq.StartupRecoveryMonitor = (*myOutboxMonitor)(nil)
    _ sqlmq.HandoffMonitor         = (*myOutboxMonitor)(nil)
)
```

## Broker-timeout sentinels report `Timeout()`

`ErrCommitTimeout`, `ErrPublishTimeout`, `ErrAcknowledgeTimeout` and
`ErrCloseTimeout` now report `Timeout() == true`. A caller can recognize any of
them, even wrapped, without importing `rabbitmq`:

```go
var timeout interface{ Timeout() bool }
if errors.As(err, &timeout) && timeout.Timeout() {
    // a broker timeout
}
```

Their declared type is still `error`. They still compare with `==` and
`errors.Is`. Only `%T` shows a different type.

## Also in this release

These changes reached master after v4.1.1. None changes library code.

- An `integration` package tests against a real three-node RabbitMQ cluster.
  It sits behind the `integration` build tag, so `make test` skips it. CI runs
  it when a tag is pushed. See "Integration tests" in the README.
- Two timeline tests reproduce a quorum queue in minority and a broker memory
  alarm. Both show that a commit or publish timeout means "unknown", not "not
  published". A message the broker already held can arrive after recovery, and
  the retry publishes it again. Consumers must be idempotent. The README and
  the v4.1.0 notes now say so.
- A flaky `sqlmq` dispatch-processor test is fixed.
