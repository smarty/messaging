---
name: Import harness + sqladapter into messaging/v3
description: Proposal to lift the infra/harness pipeline into github.com/smarty/messaging/v3/handlers/harness and relocate the SQL-bound dispatcher/writer/recovery as a sqladapter subpackage.
type: plot
---

# Proposal: Import `infra/harness` into `messaging/v3/handlers/harness`

## Background

A second project (working copy under `-context/domain-transformation-phase-9-chunk-C/code/infra`) has grown a staged, pipeline-based message-handling "harness" that we now want to promote into the shared `github.com/smarty/messaging/v3` module so it can be reused. The source split is:

- `infra/harness/*` — a generic, store-and-forward pipeline built from goroutine stages connected by buffered channels. Stages: `Entrypoint → Execution → Serialization (fan-out) → Persistence → Completion → Broadcast → Terminal`. Supporting code: `fanout.go`, `pool.go`, `routing.go`, `scanner.go`.
- `infra/*` — supporting types and a **reference implementation** of the `Writer` / `Dispatcher` interfaces plus a `Recover` function, all coupled to the same MySQL `Messages` table that `sqlmq/_schema_mysql.sql` already defines (`id`, `dispatched`, `type`, `payload`).

The target module (`github.com/smarty/messaging/v3`) already has a `handlers/` namespace (`multi`, `retry`, `sqltx`, `transactional`) and reusable pipeline/streaming infrastructure (`streaming/`, `batch/`). The harness belongs there as another composable handler pattern.

Decisions locked in during /plot and /replot:

1. **Scope:** bring in both the generic harness and the SQL-backed reference implementation, but **invert the nesting**: harness at `handlers/harness`, SQL reference impl at `handlers/harness/sqladapter`.
2. **Contracts:** reuse `messaging.Handler` and `messaging.Listener` from the root `contracts.go` instead of re-declaring them inside the harness package.
3. **Serialization:** the pipeline holds an unexported `serializer` collaborator with a `Serialize(out io.Writer, in any) error` signature (close to `encoding/json/v2`'s `MarshalWrite`); the caller supplies one. The `goexperiment.jsonv2` build tag is dropped. **No serializer implementations ship with this module** — callers own encoding.
4. **Wire-up:** `New(...)` uses the module's functional-options convention (`Options singleton` pattern — see `handlers/retry/config.go`, `batch/config.go`, `handlers/transactional/config.go`).
5. **Test library:** bring in `github.com/smarty/gunit/v2` alongside the existing v1 dep. Both major versions coexist until a later, separate module-wide upgrade to v2.

## Approach

### 1. Destination layout

```
github.com/smarty/messaging/v3/
├── contracts.go                          (unchanged; we'll reuse Handler, Listener, ListenCloser)
├── handlers/
│   ├── ...existing sub-packages...
│   └── harness/                          (NEW — generic pipeline; only New + Options are exported)
│       ├── contracts.go                   (Writer, Dispatcher, Monitor — exported collaborator types; executor, applicator, serializer — unexported internals; event structs + error sentinels)
│       ├── config.go                      (New(...) + Options singleton + option func + nop)
│       ├── message.go                     (Message value type — lifted from infra/message.go)
│       ├── pipeline.go                    (unexported build(ctx, cfg) wiring)
│       ├── 00_entrypoint.go / _test.go    (unexported entrypoint type + newEntrypoint)
│       ├── 01_execution.go  / _test.go    (unexported execution + newExecution)
│       ├── 02_serialization.go / _test.go (unexported serialization + newSerialization; no build tag)
│       ├── 03_persistence.go / _test.go   (unexported persistence + newPersistence)
│       ├── 04_completion.go  / _test.go   (unexported completion + newCompletion)
│       ├── 05_broadcast.go   / _test.go   (unexported broadcast + newBroadcast)
│       ├── 06_terminal.go    / _test.go   (unexported terminal + newTerminal)
│       ├── fanout.go                      (unexported fanIn + newFanIn + newFanOut)
│       ├── pool.go
│       ├── routing.go / routing_test.go   (unexported router + newRouter)
│       ├── scanner.go
│       └── pipeline_test.go
│       └── sqladapter/                   (NEW — SQL reference impl)
│           ├── dispatcher.go / _test.go
│           ├── writer.go     / _test.go
│           ├── recovery.go   / _test.go
│           └── contracts.go               (Logger interface)
```

### 2. Contract changes

The current `infra/harness/contracts.go` declares:

```go
type Handler  interface{ Handle(context.Context, ...any) }
type Listener interface{ Listen() }
```

These are structurally identical to the ones in `messaging/contracts.go`, so we delete the local versions and have `harness.New(...)` return `messaging.Handler` + `[]messaging.Listener`. The `entrypoint`'s `Close() error` + `Listen()` also already satisfies `messaging.ListenCloser`.

The package's collaborator interfaces split into two groups:

- **Exported** (caller supplies a real implementation via `Options.*`): `Writer`, `Dispatcher`, `Monitor`. These are the external boundaries of the pipeline.
- **Unexported** (internal to the package): `executor`, `applicator`, `serializer`, plus every pipeline-stage struct (`entrypoint`, `execution`, `serialization`, `persistence`, `completion`, `broadcast`, `terminal`), the `router`/`newRouter`, and the `fanIn`. The only way into the package is `New(...)` + `Options.*`. `Executor` and `Applicator` get unexported because callers never instantiate them directly — the pipeline discovers them reflectively via `router` from the domain types supplied through `Options.Types(...)`.

The pluggable encoder lives as an unexported interface:

```go
// serializer encodes a single message's value onto the supplied writer. The
// signature intentionally mirrors encoding/json/v2's MarshalWrite. Implementations
// must be safe for concurrent use — the pipeline runs multiple serialization workers.
type serializer interface {
    Serialize(out io.Writer, in any) error
}
```

The `serialization` stage stops importing any encoder directly; its constructor takes a `serializer`:

```go
func newSerialization(monitor Monitor, enc serializer, input, output chan *unitOfWork) *serialization
```

**No serializer implementations ship with this module.** Callers provide their own (for example, a thin wrapper around `encoding/json/v2`'s `MarshalWrite`, `encoding/json`'s `NewEncoder(w).Encode(v)`, protobuf, etc.). Keeping the type unexported forces callers to supply the collaborator through the functional option (see §8) rather than constructing a stage directly.

### 3. Events and monitor

The `Monitor` interface and its event types (`BatchInFlight`, `BatchComplete`, `UnitOfWorkInFlight`, `UnitOfWorkComplete`, `SerializationError`, `PersistenceError`, `BroadcastError`) move from `infra/contracts.go` to `handlers/harness/contracts.go` — all exported so callers can type-switch in their `Monitor.Track(any)` implementation. The error sentinels (`ErrSerialization`, `ErrPersistence`, `ErrBroadcast` — renamed from `ErrJSONSerialization` since we're format-agnostic) move with them. `Logger` is no longer needed inside `harness` itself (only `sqladapter` uses it); it moves into `sqladapter/contracts.go`.

### 4. `Message` type placement

`infra.Message` becomes `harness.Message` at `handlers/harness/message.go`. Its fields (`ID`, `Value`, `Type`, `Content`, `ContentType`, `ContentEncoding`, `Stored`, `Dispatched`) stay intact. The SQL adapter references `harness.Message` rather than duplicating.

### 5. SQL adapter (`handlers/harness/sqladapter`)

Direct port of `infra/dispatcher.go`, `infra/writer.go`, `infra/recovery.go`, with these edits:

- `package infra` → `package sqladapter`.
- Import path `root/code/infra` (implied reference) drops; cross-package uses switch to `github.com/smarty/messaging/v3/handlers/harness`.
- `messaging.Connector` / `messaging.Dispatch` references resolve against the **same** module now (no path change needed — they already import `github.com/smarty/messaging/v3`).
- Package doc comment labels it as a reference implementation that targets the `Messages` table defined by `sqlmq/_schema_mysql.sql` in this same module (columns `id`, `dispatched`, `type`, `payload`). That schema coupling is not new — `sqlmq/dispatch_store.go` already writes to the same table. The adapter is simply a second reader/writer of the same schema.
- Preserve the existing `TODO` comments (double-encoding note in `dispatcher.go`, pagination note in `recovery.go`) — those are known issues, not something this import should silently fix.
- Keep `legacyWrite func(context.Context, *sql.Tx, ...any)` escape hatch on `Writer`, but document it as deprecated in the package comment.

### 6. Test infrastructure

The source tests use **gunit v2** (`github.com/smarty/gunit/v2`) along with `assert/better` + `assert/should`. The target module currently pins **gunit v1** (`github.com/smarty/gunit v1.6.0`).

**Plan:** add `github.com/smarty/gunit/v2` to `go.mod` and port the tests with their v2 imports intact. The module will temporarily depend on both major versions. A later, separate initiative will migrate the rest of the module's tests from v1 to v2, at which point v1 can be dropped — that migration is out of scope for this import.

If any imported test uses assertions that don't exist in v2 (or have different semantics), adapt as minimally as possible; do not rewrite assertions that already work.

### 7. `go.mod` updates

- Add `github.com/smarty/gunit/v2` (latest compatible version). Both v1 and v2 will be present until the future module-wide migration.
- `go.uber.org/goleak` is in source `go.sum` — confirm during import whether any ported test actually uses it; add only if needed.
- No other new dependencies. `io` and (if caller demonstrations are ever added in docs) stdlib-only.

### 8. Functional-options wire-up

The existing `New(ctx, monitor, executor, writer, dispatcher)` signature is replaced with the module's functional-options pattern. **Only `ctx` is positional**; every collaborator — including the domain types that drive routing — flows through options. The source `Executor`/`Applicator` interfaces become unexported (`executor`, `applicator`); the reflective `router` discovers real implementations from the domain types supplied via `Options.Types(...)` and the pipeline never asks the caller for an `Executor` directly. Following the convention in `handlers/retry/config.go` and `batch/config.go`:

```go
// handlers/harness/config.go
package harness

import (
    "context"
    "io"

    "github.com/smarty/messaging/v3"
)

func New(ctx context.Context, options ...option) (messaging.Handler, []messaging.Listener) {
    var cfg configuration
    for _, apply := range Options.defaults(options...) {
        apply(&cfg)
    }
    return build(ctx, cfg) // internal wiring lives in pipeline.go
}

var Options singleton

type singleton struct{}
type option func(*configuration)

type configuration struct {
    Monitor         Monitor
    Serializer      serializer
    Writer          Writer
    Dispatcher      Dispatcher
    Types           []any // domain types (handlers/applicators); passed to newRouter
    BatchCapacity   int   // channel buffer, default 1024
    UnitSize        int   // max completions per unit, default 64
    SerializerCount int   // fan-out worker count, default 4
}

// Types registers the domain objects whose Execute.../Apply... methods drive
// the pipeline. They are passed verbatim to newRouter(...) at build time.
func (singleton) Types(value ...any) option            { return func(c *configuration) { c.Types = value } }
func (singleton) Monitor(value Monitor) option         { return func(c *configuration) { c.Monitor = value } }
func (singleton) Serializer(value serializer) option   { return func(c *configuration) { c.Serializer = value } }
func (singleton) Writer(value Writer) option           { return func(c *configuration) { c.Writer = value } }
func (singleton) Dispatcher(value Dispatcher) option   { return func(c *configuration) { c.Dispatcher = value } }
func (singleton) BatchCapacity(value int) option       { return func(c *configuration) { c.BatchCapacity = value } }
func (singleton) UnitSize(value int) option            { return func(c *configuration) { c.UnitSize = value } }
func (singleton) SerializerCount(value int) option     { return func(c *configuration) { c.SerializerCount = value } }

func (singleton) defaults(options ...option) []option {
    var blank = nop{}
    return append([]option{
        Options.Monitor(blank),
        Options.Serializer(blank),
        Options.Writer(blank),
        Options.Dispatcher(blank),
        Options.BatchCapacity(1024),
        Options.UnitSize(64),
        Options.SerializerCount(4),
    }, options...)
}

// nop satisfies every collaborator interface so New(...) can be called with
// zero options and still produce a runnable (if inert) pipeline. Callers
// override whichever collaborators they care about via Options.*.
type nop struct{}

func (nop) Track(any)                              {}
func (nop) Serialize(io.Writer, any) error         { return nil }
func (nop) Write(context.Context, ...any) error    { return nil }
func (nop) Dispatch(context.Context, ...any) error { return nil }
func (nop) Execute(any, func(...any))              {}
```

Design notes:

- `ctx` stays positional because the pipeline's lifetime is tied to it (see Trade-offs); everything else is an option.
- `Options.Types(...)` is the single entry point for registering domain behavior. `build(ctx, cfg)` constructs the internal router via `newRouter(cfg.Types...)` and passes it into the `execution` stage as its `executor` collaborator. Callers never hold a reference to the router.
- `Monitor`, `Serializer`, `Writer`, and `Dispatcher` all default to a shared `nop{}` implementation. A caller can invoke `New(ctx)` with zero options and get a runnable, inert pipeline; real deployments override whichever collaborators they care about via `Options.*`. This matches the defaulting style in `handlers/retry` and `handlers/transactional` (where `Logger` and `Monitor` default to nop).
- Buffer/worker tunables, previously hard-coded, are now options with the existing values as defaults.
- `build(ctx, cfg)` internally performs the current `New(...)`'s channel wiring; it lives in `pipeline.go` and stays unexported.
- Every pipeline-stage struct and its constructor is unexported (`entrypoint`/`newEntrypoint`, `execution`/`newExecution`, `serialization`/`newSerialization`, `persistence`/`newPersistence`, `completion`/`newCompletion`, `broadcast`/`newBroadcast`, `terminal`/`newTerminal`, `fanIn`/`newFanIn`/`newFanOut`, `router`/`newRouter`). The only way into the package is `New(...)` + `Options.*`.

### 9. Alternatives considered

- **Flat layout, no sqladapter split.** Rejected: conflates a generic pipeline with a schema-coupled reference impl; callers targeting a different schema would be forced to copy-paste rather than compose.
- **Top-level `/harness` package (sibling to `handlers/`).** Rejected: the user prefers nesting under `handlers/` where similar composable handler patterns live.
- **Absorb `Message`, `Monitor`, events into the root `messaging` package.** Rejected: these are harness-specific concepts (the root `Delivery`/`Dispatch` types are the message abstraction for the rest of the module); promoting them would blur the boundary.
- **Keep `jsonv2` build tag.** Rejected in favor of pluggable serialization. A caller who wants jsonv2 writes a short adapter; the pipeline stays encoder-agnostic.
- **Ship a JSON serializer helper subpackage.** Rejected per /replot: this module deliberately provides no serializer implementations. Callers own encoding.
- **Positional-argument constructor.** Rejected in favor of functional options for consistency with `retry`, `batch`, and `transactional`.
- **Expose stage types / `Executor` / `Router`.** Rejected: callers have no reason to construct a single stage or wire their own router. Keeping everything except `New`, `Options`, `Message`, the collaborator interfaces, and the event/error types unexported narrows the supported surface and lets us refactor internals without breaking users.
- **Positional `executor` argument.** Rejected: the source's `Executor` interface is really just "anything whose `Execute...(msg, broadcast)` methods the reflective router can see." Exposing a positional `Executor` parameter implied that callers build their own; `Options.Types(...)` more honestly describes the actual input (a list of domain objects) and lets the router stay private.

## Trade-offs & Risks

- **Context handling.** `Persistence` and `Broadcast` both capture the `ctx` supplied to `New(...)` and use it for every downstream Write/Dispatch call — so the pipeline's lifetime is tied to a single root context. This matches the source but is worth confirming explicitly; if the v3 conventions expect per-handle contexts to flow through, the stage signatures need adjustment. **Open question** for the user — proposal currently preserves source behavior.
- **Retry-forever semantics.** Persistence and Broadcast retry indefinitely with a 1-second sleep (and TODOs for exponential backoff). Imported as-is. `handlers/retry` already exists in the target module and uses more sophisticated logic; over time we may want to delegate to it, but not in this import.
- **Channel capacity defaults.** Back-pressure into the entrypoint's `Handle` (which holds a read lock while sending) can block callers under heavy load. Now tunable via `Options.BatchCapacity`, but the default of 1024 should be documented with this characteristic.
- **`reflect`-based routing.** The internal router's `Execute` mutates a shared `exclusions` slice without locking — fine in the current pipeline (routing runs inside `execution` which a single goroutine calls per message). Because the router is now unexported and constructed once per `New(...)` call, the sharing concern goes away. Add a doc comment on the internal type anyway so future contributors don't regress this invariant.
- **Dual gunit majors.** Temporarily carrying both `github.com/smarty/gunit` and `github.com/smarty/gunit/v2` increases `go.sum` noise and means new tests in *this* import use v2 while older tests elsewhere in the module still use v1. The divergence is explicit and scoped; the follow-up module-wide upgrade resolves it.
- **Silent-by-default collaborators.** Because `Serializer`, `Writer`, and `Dispatcher` all default to a `nop{}` that returns `nil`, forgetting to wire a real implementation produces a pipeline that silently drops work rather than surfacing an error. Likewise, forgetting `Options.Types(...)` produces a pipeline whose router has no registered handlers. Callers must remember to set these. Mitigate by documenting this in the package doc comment and the `New(...)` godoc, and by making the `Options.*` entries the obvious next step in any example.

## Implementation Checklist

### Phase 1: Scaffolding and contracts

- [x] Create directory `handlers/harness/` and `handlers/harness/sqladapter/`.
- [x] Add `github.com/smarty/gunit/v2` to `go.mod`; run `go mod tidy`. (Temporarily removed by tidy with no importers; will return automatically once Phase 2 tests are ported.)
- [x] Write `handlers/harness/contracts.go` with the **exported** surface (`Writer`, `Dispatcher`, `Monitor`, event structs `BatchInFlight`, `BatchComplete`, `UnitOfWorkInFlight`, `UnitOfWorkComplete`, `SerializationError`, `PersistenceError`, `BroadcastError`, sentinels `ErrSerialization`, `ErrPersistence`, `ErrBroadcast`) plus the **unexported** internal interfaces (`executor`, `applicator`, `serializer`) and value types (`batch`, `unitOfWork`).
- [x] Write `handlers/harness/message.go` — copy `Message` struct with doc comments intact.
- [x] Write `handlers/harness/pool.go` — copy verbatim (no external deps).
- [x] Write `handlers/harness/scanner.go` — copy verbatim (signatures adjusted to use unexported `executor`/`applicator`).
- [x] Write `handlers/harness/fanout.go` — `fanIn` / `newFanIn` / `newFanOut` stay unexported; `stationFactory` now returns `messaging.Listener` and uses unexported `unitOfWork`.
- [x] Run `make compile` — confirm the package compiles (no tests yet).

### Phase 2: Port stages bottom-up, TDD each one

Work stage-by-stage from the terminal stage (simplest) upward, since downstream stages have no dependencies on upstream stages. All tests use `gunit/v2` imports as-is from source. **All stage types and constructors are renamed to unexported forms** (`Terminal` → `terminal`, `NewTerminal` → `newTerminal`, etc.); since the tests live in the same package, `_test.go` files can see them.

- [x] Copy `06_terminal_test.go` from source; rename referenced types to lowercase. Run the terminal test — expect **failure** (`terminal` type doesn't exist yet in this package).
- [x] Port `06_terminal.go` as `terminal` / `newTerminal`. Run tests; confirm passing.
- [x] Copy `04_completion_test.go`; lowercase the type references. Run — expect failure.
- [x] Port `04_completion.go` as `completion` / `newCompletion`. Run tests; confirm passing.
- [x] Copy `05_broadcast_test.go`; lowercase the type references. Run — expect failure.
- [x] Port `05_broadcast.go` as `broadcast` / `newBroadcast`. Run; confirm passing.
- [x] Copy `03_persistence_test.go`; lowercase the type references. Run — expect failure.
- [x] Port `03_persistence.go` as `persistence` / `newPersistence`. Run; confirm passing.
- [x] **Rewrite** `02_serialization_test.go` to use a fake `serializer` (not jsonv2). Test should cover: success path writes to `message.Content`; serializer error is reported via monitor as `SerializationError` with `ErrSerialization` wrapped. Run — expect failure.
- [x] Port `02_serialization.go` as `serialization` / `newSerialization` with the new `Serialize(io.Writer, any) error` signature; drop the `//go:build goexperiment.jsonv2` tag. Run; confirm passing.
- [x] Copy `01_execution_test.go`; lowercase the type references. Run — expect failure.
- [x] Port `01_execution.go` as `execution` / `newExecution`. Run; confirm passing.
- [x] Copy `00_entrypoint_test.go`; lowercase the type references. Confirm it exercises `Close` + `Listen` semantics. Run — expect failure.
- [x] Port `00_entrypoint.go` as `entrypoint` / `newEntrypoint`. Run; confirm passing.

### Phase 3: Routing, pipeline, and functional-options config

- [x] Copy `routing_test.go`; lowercase references (`Router` → `router`, `NewRouter` → `newRouter`). Run — expect failure.
- [x] Port `routing.go` as `router` / `newRouter` with unexported `executor` / `applicator` interfaces; update `scanner.go`'s signature to match. Run; confirm passing.
- [x] Write `handlers/harness/pipeline.go` as the unexported `build(ctx, cfg)` function; it constructs all the channels, calls `newRouter(cfg.Types...)`, wires every stage, and returns `messaging.Handler` + `[]messaging.Listener`.
- [x] Write `handlers/harness/config.go` with `New(ctx, options...)` (no positional executor), `Options singleton`, `option` type, `configuration` struct (including `Types []any`), and per-option setters per §8, including `Options.Types(...)`.
- [x] Write a `config_test.go` that asserts defaults (`BatchCapacity=1024`, `UnitSize=64`, `SerializerCount=4`, and that `Monitor`, `Serializer`, `Writer`, and `Dispatcher` all default to the shared `nop{}` and behave inertly when the pipeline runs with no options supplied). Also assert that `Options.Types(...)` populates `configuration.Types` verbatim.
- [x] Adapt the source `pipeline_test.go` to call the new functional-options `New(...)`: the fixture registers itself via `Options.Types(this)` (it implements the `Execute...` method), and supplies fake `Writer`/`Dispatcher`/`serializer`/`Monitor` via `Options.*`. Run — expect failure if anything is still misaligned, then make green.
- [x] Run the full harness test suite with `-race`; confirm no goroutine leaks and all tests pass.

### Phase 4: SQL adapter

- [x] Copy `infra/dispatcher_test.go` to `handlers/harness/sqladapter/dispatcher_test.go`. Rewrite imports (`package infra` → `sqladapter`, `infra.Message` → `harness.Message`; gunit imports stay v2). Also added `testdb_test.go` with local `openTestDatabase`/`ensureDatabaseReadiness` helpers (pointing at `sqlmq/_schema_mysql.sql`) since the source's `db-connector` dep is out of scope for this module. Run — expect failure.
- [x] Port `dispatcher.go` to `sqladapter/dispatcher.go`. Add package-level doc comment labeling it as a reference implementation targeting the `Messages` table defined by `sqlmq/_schema_mysql.sql`. Preserve existing TODOs. Run; confirm passing.
- [x] Copy `writer_test.go` → `sqladapter/writer_test.go`, rewrite imports. Replaced the external `billing` registry types and `openTestDatabase` dep with local test-only `orderReceived`/`orderApproved` structs and shared helpers from `testdb_test.go`. Run — expect failure.
- [x] Port `writer.go`. Preserve `legacyWrite` escape hatch with a deprecation note in the godoc. Run; confirm passing.
- [x] Copy `recovery_test.go` → `sqladapter/recovery_test.go`, rewrite imports. Run — expect failure.
- [x] Port `recovery.go`. Preserve existing TODOs about pagination and Listener conversion. Run; confirm passing.
- [x] Add `sqladapter/contracts.go` with the `Logger` interface (moved from `infra/contracts.go`). (Added earlier alongside dispatcher.go so it would compile.)

### Phase 5: Module hygiene

- [x] Run `make test` — full module test suite with `-race -covermode=atomic`. Confirm green. (All packages pass; harness 99.5%, sqladapter 0% under `-short` since its integration tests require a live MySQL — they pass end-to-end against a local DB when run without `-short`.)
- [x] Run `go mod tidy`. Confirms two new direct deps: `github.com/smarty/gunit/v2` (planned) and `github.com/go-sql-driver/mysql` (test-only driver added during Phase 4 when we chose not to bring in `db-connector` — imported via `testdb_test.go`'s `_` alias, so it participates only in test builds). One new indirect: `filippo.io/edwards25519` (transitively required by the MySQL driver).
- [x] Inspect `go.sum` diff — only `filippo.io/edwards25519` and gunit/v2 additions, both explained by the items above.
- [x] Grep the new packages for references to `root/code/infra` — confirm zero.
- [x] Grep the new packages for any `goexperiment.jsonv2` build tags — confirm zero.
- [x] Add short package-level doc comments (`// Package harness provides a staged, store-and-forward message-handling pipeline...` etc.) on `harness` and `sqladapter`. (`harness` on config.go, `sqladapter` on dispatcher.go.)
- [x] Self-review diff for any stray `package infra` / wrong package declarations, unused imports, and leftover `TODO: pool ...` comments that should stay vs. be addressed now (keep them — they're load-bearing signals for future work).