# Release notes: v4.0.0

This release hardens RabbitMQ publishing against silent stalls. It is a major
version because safe adoption requires deliberate action from each caller. The
status checker changes its semantics. Services must verify their restart
thresholds before they upgrade. A major version makes an accidental upgrade
(via `go get -u`) impossible. See the proposal in `doc/work-sessions/2026/`
for the full background.

## Migration steps

1. Rewrite imports: `github.com/smarty/messaging/v3` becomes
   `github.com/smarty/messaging/v4` (including the `go.mod` requirement).
2. Add two methods to your `rabbitmq.Options.Monitor` implementation:
   `ConnectionBlocked(reason string)` and `ConnectionUnblocked()`. Empty
   bodies are sufficient. See below for the recommended wiring.
3. If you implement or fake `rabbitmq/adapter.Connection`, add one method:
   `BlockedNotifications() <-chan amqp.Blocking`. A fake can return nil.
4. Read the status-checker section below. Confirm your platform's reaction to
   a failing `/status` before you deploy.

Note: the old `v4.0.0-alpha.*` tags belong to an abandoned 2021 streaming
experiment. This release retracts them; they are unrelated to this line.

## Behavior change: honest status checker, with failure tolerance

`status.Checker.Status` now reports probe errors. Before this release, it
swallowed every error except password errors. A service with an unreachable
broker now fails `/status`, and the platform reacts according to its own
policy. A restart is the intended reaction for a stalled publisher. The
restart runs outbox recovery and drains the backlog.

To prevent a brief broker outage from causing an unnecessary restart, the
checker tolerates probe failures inside a tolerance window (**default: 30
seconds**). While consecutive probes fail inside the tolerance window,
`Status` returns nil and logs a `[WARN]` line. Once failures persist past the
tolerance window, `Status` reports the error. Any success resets the tolerance
window. The tolerance window is wall-clock time, so it does not depend on the
probe cadence configured in `httpstatus`. Configure it per service:

```go
status.New(
    status.Options.Connector(connector),
    status.Options.FailureTolerance(45*time.Second),
)
```

`FailureTolerance(0)` reports the first failure. Before each service rollout,
size the tolerance window just above your longest routine broker event.
Routine events include a rolling restart and a failover election.

Definitive errors bypass the tolerance window. An AMQP `ACCESS_REFUSED` (403)
or `NOT_ALLOWED` (530) error reports on the first probe, because no retry can
fix a configuration fault. These errors include:

- bad credentials
- a missing vhost
- a denied permission

This rule restores and broadens the v3 behavior that returned password errors
immediately.

## New: explicit client heartbeat

The client now requests an AMQP heartbeat of **10 seconds** by default. A dead
socket now surfaces as an error within about 15 seconds. The client sends a
heartbeat every half interval and enforces a read deadline of three sends
(1.5 x the negotiated interval). This protection holds even when the broker or
a proxy offers no heartbeat. Configure the interval with:

```go
rabbitmq.New(rabbitmq.Options.Heartbeat(30 * time.Second))
```

To restore the previous wire behavior, configure
`rabbitmq.Options.Heartbeat(0)`. The client then defers to the interval the
broker offers. Only an explicit 0 does that, because the option sanitizes its
input. A positive value below one second rounds up to one second, because the
wire protocol carries whole seconds. The option replaces a negative value with
the 10-second default. A computed or misparsed value cannot silently disable
the heartbeat.

## New (and breaking): broker blocked-connection notifications

The connection now registers `NotifyBlocked` with the broker. When a memory or
disk alarm blocks the connection, the library logs:

- `[WARN] AMQP connection blocked by broker (reason: ...)` when the block starts.
- `[INFO] AMQP connection unblocked by broker; publishes resume.` when it ends.

The `monitor` contract now includes these events. Every monitor implements:

```go
func (this *myMonitor) ConnectionBlocked(reason string) { ... }
func (this *myMonitor) ConnectionUnblocked()            { ... }
```

Empty bodies satisfy the contract. A blocked broker does not fail `/status`
immediately, because the broker keeps accepting probe bytes into its buffers.
Wire these callbacks to a gauge and an alert. Paging is the correct reaction
to a blocked broker. A restart does not help.

A long block eventually stalls the probe's socket write. The probe then honors
its context deadline and severs its connection. A 5-second socket deadline
bounds the connection close. `/status` reports the failure once failures
persist past the tolerance window.

## Breaking: `adapter.Connection` interface addition

The `rabbitmq/adapter.Connection` interface gains one method:

```go
BlockedNotifications() <-chan amqp.Blocking
```

The real adapter registers the channel with the broker during `Connect`. This
timing closes the registration window that could drop a notification. Custom
implementations of this interface (test fakes included) must add this method.
A fake can return nil.
