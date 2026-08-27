# Release notes: v3.18.0

This release hardens RabbitMQ publishing against silent stalls. See the
proposal in `doc/work-sessions/2026/` for the full background.

## New: explicit client heartbeat

The client now requests an AMQP heartbeat of **10 seconds** by default. A dead
socket now surfaces as an error within about 30 seconds, even when the broker
or a proxy offers no heartbeat. Configure the interval with:

```go
rabbitmq.New(rabbitmq.Options.Heartbeat(30 * time.Second))
```

## New: broker blocked-connection notifications

The connection now registers `NotifyBlocked` with the broker. When a memory or
disk alarm blocks the connection, the library logs:

- `[WARN] AMQP connection blocked by broker (reason: ...)` when the block starts.
- `[INFO] AMQP connection unblocked by broker; publishes resume.` when it ends.

A monitor can opt in to these events. Add these two methods to your monitor:

```go
func (this *myMonitor) ConnectionBlocked(reason string) { ... }
func (this *myMonitor) ConnectionUnblocked()            { ... }
```

Existing monitors compile unchanged. The methods are optional.

## Breaking: `adapter.Connection` interface addition (T3)

The `rabbitmq/adapter.Connection` interface gains one method:

```go
NotifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking
```

Custom implementations of this interface (test fakes included) must add this
method. A one-line delegation or `return receiver` is sufficient.

## Behavior change: honest status checker (T1)

`status.Checker.Status` now returns **every** error from its probe. Before
this release, it swallowed all errors except password errors. A service with
an unreachable broker now fails `/status`.

Before you upgrade a service, confirm that its `httpstatus` failure threshold
tolerates a brief broker blip. A restart on sustained failure is the intended
reaction: it runs outbox recovery and drains the backlog.
