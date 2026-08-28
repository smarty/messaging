# Release notes: v4.0.0

This release hardens RabbitMQ publishing against silent stalls. It is a major
version because safe adoption requires deliberate caller-side action: the
status checker's semantics change, and services must verify their restart
thresholds before they upgrade. A major version makes an accidental upgrade
(via `go get -u`) impossible. See the proposal in `doc/work-sessions/2026/`
for the full background.

## Migration steps

1. Rewrite imports: `github.com/smarty/messaging/v3` becomes
   `github.com/smarty/messaging/v4` (including the `go.mod` requirement).
2. Add two methods to your `rabbitmq.Options.Monitor` implementation:
   `ConnectionBlocked(reason string)` and `ConnectionUnblocked()` (empty
   bodies are sufficient; see below for the recommended wiring).
3. If you implement or fake `rabbitmq/adapter.Connection`, add one method:
   `BlockedNotifications() <-chan amqp.Blocking`
   (`return nil` is a sufficient fake).
4. Read the status-checker section below and confirm your platform's reaction
   to a failing `/status` before you deploy.

Note: the old `v4.0.0-alpha.*` tags belong to an abandoned 2021 streaming
experiment. This release retracts them; they are unrelated to this line.

## Behavior change: honest status checker, with failure tolerance

`status.Checker.Status` now reports probe errors. Before this release, it
swallowed every error except password errors. A service with an unreachable
broker now fails `/status`, and the platform reacts according to its own
policy. For a wedged publisher, a restart is the intended reaction: it runs
outbox recovery and drains the backlog.

To prevent a brief broker blip from causing a hasty restart, the checker
tolerates probe failures for a duration (**default: 30 seconds**). While
consecutive probes keep failing inside that window, `Status` returns nil and
logs a `[WARN]` line; once failures have persisted past the window, `Status`
reports the error. Any success resets the window. The tolerance is wall-clock
time, so it is independent of the probe cadence configured in `httpstatus`.
Configure it per service:

```go
status.New(
    status.Options.Connector(connector),
    status.Options.FailureTolerance(45*time.Second),
)
```

`FailureTolerance(0)` reports the first failure. Before each service rollout,
size the window just above your longest routine broker event (a rolling
restart or a failover election).

Definitive errors bypass the window: an AMQP `ACCESS_REFUSED` (403) or
`NOT_ALLOWED` (530) error — bad credentials, a missing vhost, a denied
permission — reports on the first probe, because no retry can fix a
configuration fault. This restores (and broadens) the v3 behavior that
returned password errors immediately.

## New: explicit client heartbeat

The client now requests an AMQP heartbeat of **10 seconds** by default. A dead
socket now surfaces as an error within about 15 seconds — the client sends a
heartbeat every half interval and enforces a read deadline of three of those
sends (1.5 x the negotiated interval) — even when the broker or a proxy offers
no heartbeat. Configure the interval with:

```go
rabbitmq.New(rabbitmq.Options.Heartbeat(30 * time.Second))
```

Escape hatch: `rabbitmq.Options.Heartbeat(0)` restores the previous wire
behavior (the client defers to the interval the broker offers). Only an
explicit 0 does that: the option sanitizes its input, so a positive value
below one second rounds up to one second (the wire protocol carries whole
seconds) and a negative value is replaced by the 10-second default. A
computed or misparsed value cannot silently disable the heartbeat.

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

Empty bodies satisfy the contract. A blocked
broker does not fail `/status` immediately (the broker keeps accepting probe
bytes into its buffers), so wire these callbacks to a gauge and an alert;
paging is the correct reaction to a blocked broker, and a restart does not
help. If a block persists long enough to wedge the probe's socket write, the
probe honors its context deadline, severs its connection (connection close is
bounded by a 5-second socket deadline), and `/status` reports the failure once
the tolerance window passes.

## Breaking: `adapter.Connection` interface addition

The `rabbitmq/adapter.Connection` interface gains one method:

```go
BlockedNotifications() <-chan amqp.Blocking
```

The real adapter registers the channel with the broker during `Connect`, so no
notification is dropped in a registration window. Custom implementations of
this interface (test fakes included) must add this method; `return nil` is a
sufficient fake.
