# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
make test                      # go mod tidy, go fmt, then go test -timeout=1s -short -race -covermode=atomic ./...
make compile                   # go build ./...
make build                     # test + compile (this is what CI runs)
go test -timeout=1s -short -race ./sqlmq/                                   # one package
go test -timeout=1s -short -race -run 'TestDispatchReceiverFixture' ./sqlmq/ # one gunit fixture
go test -timeout=1s -short -race -run 'TestDispatchReceiverFixture/TestWhenHandoffExceedsTimeout' ./sqlmq/  # one test
```

The suite runs under a **1-second global timeout**. Any test that sleeps or waits must use single-digit
millisecond durations against fakes that block until released. A test that hangs kills the whole package
binary, so a "RED" step for a blocking bug often shows up as a suite timeout rather than an assertion.

Tests use `github.com/smarty/gunit` fixtures (`gunit.Run(new(XFixture), t)`, methods named `Test*`,
`Setup`/`Teardown`, `this.So(actual, should.X, expected)`). Subtests inside a fixture run in parallel, so
fixture state must be per-test. Each fixture typically implements the package's own interfaces (adapter
channel, message store, monitor, logger) as its fakes.

## Architecture

`contracts.go` at the root defines the transport-neutral vocabulary and nothing else:
`Connector -> Connection -> {Reader -> Stream, Writer, CommitWriter}`, plus the `Dispatch` (outbound) and
`Delivery` (inbound) structs, `Handler`, and `ListenCloser`. Every other package either **implements** a
`Connector` for a transport, **decorates** a `Connector`, or **consumes** one.

| Package                  | Role                                                                                                   |
|--------------------------|--------------------------------------------------------------------------------------------------------|
| `rabbitmq`               | Transport. `Connector` over `amqp091-go`, via the thin `rabbitmq/adapter` interfaces so tests can fake the broker. |
| `serialization`          | Decorator. Encodes `Dispatch.Message` to `Payload` on write and decodes `Delivery.Payload` to `Message` on read, using `WriteTypes`/`ReadTypes` registries. JSON by default. |
| `sqlmq`                  | Decorator + outbox. Its `CommitWriter` stores dispatches in a SQL `Messages` table inside the caller's transaction; a background `dispatchProcessor` publishes them through the wrapped transport and marks them dispatched. |
| `batch`                  | `Writer` that does connect / CommitWriter / Write / Commit as one publish and redials after any error. `sqlmq` uses it as the outbox sender. |
| `streaming`              | Consumer runtime. Opens a `Stream` per subscription, fans deliveries to one goroutine per handler, batches, calls `Handler.Handle`, acknowledges. Owns reconnect and the soft/hard shutdown contexts. Has its own per-stream `monitor`. |
| `handlers/transactional` | Wraps a `Handler` in a `CommitWriter` from the given connector. When that connector is `sqlmq`, the SQL `*sql.Tx` is deposited into the context via `Store(*sql.Tx)` and handed to the handler factory as `State{Tx, Writer}`. Fails by **panicking**. |
| `handlers/retry`         | Recovers panics from the inner handler and retries with backoff. This is the only thing that turns a `transactional` panic into a retry. |
| `handlers/sqltx`         | Like `transactional` but for a bare `*sql.DB` with no messaging. |
| `handlers/multi`         | Fans one batch to several handlers in order. |
| `status`                 | Health probe: publishes an empty dispatch; tolerates failures inside a window; reports 403/530 at once. |

### Composition order (enforced by the types)

```
rabbitmq.New(...)                                -> transport Connector
serialization.New(transport, ...)                -> encoded Connector (must wrap BEFORE sqlmq so outbox rows are stored serialized)
sqlmq.New(encoded, ...)                          -> (outbox Connector, dispatcher ListenCloser)
transactional.New(outbox, factory)               -> Handler (panics on failure)
retry.New(thatHandler, ...)                      -> Handler (recovers, backs off)  -- retry wraps transactional, never the reverse
streaming.New(encoded, Options.Subscriptions(NewSubscription(queue, SubscriptionOptions.AddWorkers(handler...))))
```

Run `dispatcher.Listen()` and the streaming `Listen()` in their own goroutines; `Close()` each to stop.

### The panic-as-error-channel convention

`Handler.Handle` returns nothing. Failure propagates by panic: `transactional` and `sqltx` panic on connect,
begin, or commit errors and re-panic after rollback; `retry` recovers and re-runs the batch; `streaming`
acknowledges only after `Handle` returns. Consequently **any code that runs after a SQL commit must not
return or raise an error**, or the batch is redelivered and its side effects run twice. `sqlmq`'s
`dispatchReceiver.Commit` returns `nil` on every path after the SQL commit for exactly this reason.

### Outbox data flow (`sqlmq`)

1. Handler writes dispatches to the `sqlmq` `CommitWriter`; they are buffered in memory.
2. `Commit` stores the rows in `Messages` (same `*sql.Tx` as the handler's own work), commits SQL, then
   hands the dispatches to a `chan messaging.Dispatch` shared with the processor. The handoff is bounded
   by `HandoffTimeout`; leftovers move to a capped background `deferredHandoffs` goroutine.
3. `dispatchProcessor` drains the channel, publishes through `batch.Writer` over the wrapped transport,
   then `Confirm`s (sets `dispatched`). On any error it sleeps `RetryTimeout` and retries the same buffer.
4. At startup `readPending` loads undispatched rows and feeds them into the channel. There is no periodic
   table sweep because many service instances share one table.

### Every wait has a bound

`rabbitmq.Options.CommitTimeout` (30 s) severs the connection to unblock a stuck `TxCommit`/`TxRollback`;
`sqlmq.Options.HandoffTimeout` (10 s) and `DeferredHandoffCapacity` (8192) bound the post-commit handoff.
Options that carry a bound sanitize zero/negative input back to the default so a bound cannot be
disabled by accident. Keep that property when adding options. See `doc/release-notes-v4.1.0.md`.

## Conventions specific to this repo

- **Options pattern** in every configurable package: `var Options singleton`, `type option func(*configuration)`,
  `Options.apply(options...)` which prepends `Options.defaults(...)` so caller values win, a `nop` type that
  satisfies the package's `logger` and `monitor`. `handlers/*` inline the defaults loop in `New` instead of
  `apply`; `streaming` has a second family, `SubscriptionOptions`, whose `apply` panics on invalid input.
- **`streaming` observability**: a `monitor` whose callbacks all carry the stream (queue) name so callers
  can label per subscription, plus log lines at each failure path (connect, reader, stream, acknowledge,
  forced shutdown, reconnect). `BatchHandled`/`BatchAcknowledged` fire per batch; keep that path cheap.
  `workerConfig.Now` exists so tests can control measured durations.
- **Per-package `monitor` and `logger` interfaces** are unexported and defined in each package's
  `contracts.go`. Adding a method to a `monitor` interface breaks every implementer and is a major-version
  change. Prefer a new sentinel error through an existing callback (as `ErrCommitTimeout` does), or log-only.
- **Log line format** is `[LEVEL] Sentence.` with bracketed values, e.g. `[WARN] Unable to commit channel transaction [%s].`
  Levels used: `INFO` for expected conditions (shutdown, recovery counts), `WARN` for anything that needs a
  human eventually.
- **Adapter seams**: `rabbitmq/adapter` and `sqlmq/adapter` exist so the packages above them can be tested
  with fakes. Changing an adapter interface is a breaking change for consumers who fake it.
- **Docs**: design work lives in `doc/work-sessions/<year>/*.html` (proposals with implementation
  checklists) and each release gets `doc/release-notes-vX.Y.Z.md` written in plain, short-sentence style.
- Go version: `go.mod` says 1.25; CI uses `go-version: stable`.
