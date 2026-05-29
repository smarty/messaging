---
name: Harness pipeline resilience (module-local changes)
description: Implement the messaging/v3 harness side of the cross-repo "harness resilience and idempotency" proposal — split-path entrypoint (Handle vs HandleResult), depth-based admission shedding for HTTP, and split channel-buffer sizing (BatchCapacity vs UnitCapacity). Excludes billing-context HTTP shell changes and post-deploy observation, which belong to the consuming repo.
type: plot
---

# Proposal: Harness Pipeline Resilience — Module-Local Changes

## Background

The cross-repo proposal at
`billing-context/.../2026-05-15_23-02-32-proposal-harness-resilience-and-idempotency.md`
describes three operational changes layered on top of Chunk C of the incremental
domain transformation. Two of those changes ("two-path entrypoint" and "split
channel buffer sizing") live entirely inside this module
(`github.com/smarty/messaging/v3`); a third change ("HTTP shell outcome
mapping") lives in the consuming repo (`billing-context`) and is out of scope
here.

This proposal scopes the work to just the parts that land in *this* repo:

- All edits under `handlers/harness/` (entrypoint, contracts, config, pipeline,
  fanout, and their tests).
- A documentation update to `doc/work-sessions/2026/2026-05-14_pipeline-component-diagram.svg`
  to reflect the split capacity knobs and the new monitor observations.

The HTTP shell wireup, integration tests, error definitions, monitor metric
registration, and post-deploy observation steps from the parent proposal happen
in `billing-context` *after* this repo's changes merge and a tagged version of
`messaging/v3` is published. They are explicitly **out of scope** for this
proposal — they will be picked up under a follow-on session in that repo.

### Why the changes are needed (recap)

Two operational concerns surfaced after Chunks A and B of the incremental
domain transformation merged:

1. **HTTP requests stack up indefinitely during a database/RabbitMQ outage.**
   `entrypoint.Handle` blocks on a per-call `sync.WaitGroup` until the
   Completion stage fires; the caller's `context.Context` is captured into
   `*batch` but never observed. Even when the HTTP client's deadline passes or
   the load balancer cancels the request, the goroutine stays parked.
2. **The pipeline's six channels are all sized to one knob (`BatchCapacity`,
   default 1024).** During an outage that lets tens of thousands of in-memory
   domain mutations sit between Apply and durable storage. The in-memory
   `Domain` drifts far ahead of what was ever stored, surfacing as "we said yes
   to the client, then forgot" on restart.

The harness-side fix has three independently-mergeable pieces:

1. **Split `Handle` into two paths**: the existing `Handle(ctx, msgs...)` for
   MQ/cron callers (preserves today's blocking, contract-honoring behavior),
   and a new `HandleResult(ctx, message any) HandleOutcome` for HTTP callers
   (honors `ctx.Done()`, applies admission shedding, returns a status outcome,
   and accepts exactly one message per call).
2. **Depth-based admission inside `HandleResult`** — high-watermark check plus
   hard-full backstop, both HTTP-only.
3. **Split channel-buffer sizing** — `BatchCapacity` continues to size the
   caller-side `batches` channel; a new `UnitCapacity` (default 1) sizes
   `work1`–`work5` and the per-worker fan-out outputs.

The companion `AdjustOrder` domain idempotency change (separate proposal,
already merged in the consuming repo) makes it safe for `HandleResult` to
return early when the caller's `ctx` fires: the in-flight batch keeps
processing and durably stores, and a client retry collapses to a no-op once
the original batch has persisted.

## Approach

### Decision summary

Two distinct, parallel methods on `*entrypoint`:

| Method                                            | Caller   | Honors `ctx.Done()` | Applies shed | Return value            | Message arity     |
|---------------------------------------------------|----------|---------------------|--------------|-------------------------|-------------------|
| `Handle(ctx, messages ...any)`                    | MQ, cron | No                  | No           | none (today's contract) | variadic (1..N)   |
| `HandleResult(ctx, message any) HandleOutcome`    | HTTP     | Yes                 | Yes          | `HandleOutcome`         | exactly one       |

`HandleResult` takes a single `message any` (not variadic) because every HTTP
route in the consuming repo invokes the domain with exactly one command per
request. Constraining the signature at the entrypoint:

- Eliminates the empty-slice / multi-message edge cases on the HTTP path
  (no need to defend against `HandleResult(ctx)` or `HandleResult(ctx, a, b)`
  in either the entrypoint or any call site).
- Tightens the worst-case in-memory work-in-progress bound: combined with
  `UnitCapacity=1`, each in-flight HTTP request contributes exactly one input
  message to a batch rather than potentially many. The `batches` channel
  capacity now corresponds directly to a count of HTTP commands enqueued
  rather than a count of caller invocations of arbitrary size.
- Surfaces the asymmetry plainly in the type system — MQ/cron may legitimately
  need to deliver multiple events per call (e.g. broker batch deliveries),
  HTTP does not.

`HandleOutcome` is an `int`-backed enum:

| Value                       | Meaning                                                                                                                                                        |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `OutcomeAccepted` (0)       | Batch admitted *and* completion fired before the caller's ctx expired. Mutation durably stored.                                                                |
| `OutcomeShed` (1)           | Batch refused at admission (high-watermark, hard-full, or pipeline closed). Pipeline never saw the batch.                                                      |
| `OutcomeCallerDeparted` (2) | Batch admitted but the caller's `ctx` fired before completion. Pipeline still owns the batch and will durably store; caller does not know storage status yet.  |

Two new monitor observations:

- `LoadShed{}` — emitted by `HandleResult` when admission is rejected.
- `CallerDeparted{}` — emitted by `HandleResult` when the caller's `ctx` fired
  before completion.

Two new configuration options with defaults:

- `Options.UnitCapacity(int)` — default `1`. Sizes `work1`–`work5` and the
  per-worker fan-out outputs.
- `Options.ShedThreshold(float64)` — default `0.80`. Fraction of the `batches`
  channel capacity past which `HandleResult` sheds.

### Detailed design

#### 1. Two-path entrypoint with shared helpers

Today's `00_entrypoint.go:29` has a single `Handle` whose body inlines waiter
acquisition, batch allocation, completion-callback wiring, the
admission-under-RWMutex sequence, and the `waiter.Wait()` block. The split
extracts three private helpers and gives each method its own admission and
waiting logic. The `prepare` helper keeps its variadic `...any` shape so
`Handle` can pass its argument through verbatim; `HandleResult` calls
`prepare(ctx, message)` with its single message, which Go promotes to a
one-element slice at the call site.

```go
// prepare acquires a waiter, allocates a *batch from the pool, and wires up
// the completion callback. Returns the items the caller will need.
func (this *entrypoint) prepare(ctx context.Context, messages ...any) (waiter *sync.WaitGroup, item *batch) {
    waiter = this.waiters.Get()
    waiter.Add(1)
    item = this.batches.Get()
    item.ctx = ctx
    item.messages = messages
    item.complete = func() {
        waiter.Done()
        this.monitor.Track(batchComplete)
        this.batches.Put(item)
    }
    return waiter, item
}

// abandon releases waiter and pool entry when the item was never enqueued
// (i.e. complete() will never fire).
func (this *entrypoint) abandon(waiter *sync.WaitGroup, item *batch) {
    waiter.Done()
    this.batches.Put(item)
}

// waiterDone wraps waiter.Wait() in a chan struct{} so it's select-able.
func (this *entrypoint) waiterDone(waiter *sync.WaitGroup) (done chan struct{}) {
    done = make(chan struct{})
    go func() { waiter.Wait(); close(done) }()
    return done
}
```

**Path A — `Handle` (MQ and cron):** preserves today's contract verbatim.

```go
func (this *entrypoint) Handle(ctx context.Context, messages ...any) {
    waiter, item := this.prepare(ctx, messages...)
    defer this.waiters.Put(waiter)

    this.lock.RLock()
    if this.closed {
        this.lock.RUnlock()
        this.abandon(waiter, item)
        return
    }
    this.work <- item
    this.monitor.Track(batchInFlight)
    this.lock.RUnlock()

    waiter.Wait()
}
```

Properties:
- **No `ctx.Done()` honoring.** MQ deliveries don't carry a client deadline;
  cron has its own scheduler-level guard. Returning early would cause
  `streaming` to ack work that the pipeline never finished.
- **No load-shed.** Sending to `this.work` is a blocking channel send.
  Back-pressure naturally propagates to the broker via prefetch limits and
  unacked-message counts.
- **Hard-full backstop is gone for this path.** The only "shed" condition is
  pipeline shutdown (`this.closed`) — exactly today's behavior.

**Path B — `HandleResult` (HTTP):** new behavior, accepts a single message.

```go
func (this *entrypoint) HandleResult(ctx context.Context, message any) HandleOutcome {
    waiter, item := this.prepare(ctx, message)
    defer this.waiters.Put(waiter)

    this.lock.RLock()
    if this.closed {
        this.lock.RUnlock()
        this.abandon(waiter, item)
        return OutcomeShed
    }
    if float64(len(this.work))/float64(cap(this.work)) >= this.shedThreshold {
        this.lock.RUnlock()
        this.abandon(waiter, item)
        this.monitor.Track(loadShed)
        return OutcomeShed
    }
    select {
    case this.work <- item:
        this.monitor.Track(batchInFlight)
    default:
        this.lock.RUnlock()
        this.abandon(waiter, item)
        this.monitor.Track(loadShed)
        return OutcomeShed
    }
    this.lock.RUnlock()

    select {
    case <-this.waiterDone(waiter):
        return OutcomeAccepted
    case <-ctx.Done():
        this.monitor.Track(callerDeparted)
        return OutcomeCallerDeparted
    }
}
```

Properties:
- **Single message per call.** The signature accepts exactly one `message any`,
  matching every HTTP-driven domain command in the consuming repo.
- **Honors `ctx.Done()`** — HTTP request deadlines unblock the caller goroutine.
- **Applies high-watermark + hard-full shed** — fast-fail for HTTP without
  blocking.
- **Outcome-returning** — caller (HTTP shell, in another repo) maps to
  status codes.

**Pool-entry lifecycle (consistent across both paths):**
- Success path: pipeline's Completion stage invokes `item.complete()`, which
  calls `this.batches.Put(item)`. Neither method puts the item back itself.
- Caller-departed path (only reachable in `HandleResult`): the pipeline still
  owns the item and will eventually invoke `item.complete()`. `HandleResult`
  must not `Put` — the pool would receive the same item twice.
- Shed and closed-pipeline paths: the item was never enqueued, so `complete()`
  will never fire. `abandon(waiter, item)` does the cleanup.

**Critically, the batch is not abandoned by the pipeline when the caller
departs.** When `OutcomeCallerDeparted` is returned, the in-flight batch
keeps processing; `complete()` still fires; persistence still happens. Only
the HTTP caller's goroutine returns early.

#### 2. Depth-based admission (HTTP-only, lives inside `HandleResult`)

The high-watermark check (`len/cap >= shedThreshold`) and the hard-full
backstop (the `default` arm of the `select` on `this.work <- item`) both live
inside `HandleResult`. `Handle` (MQ/cron) implements neither — see §1 for why.

`Options.ShedThreshold(value float64)` exposes the threshold; default `0.80`.
Setting it ≥ `1.0` disables high-watermark shedding (only the hard-full
backstop remains, also HTTP-only).

The `len(chan)/cap(chan)` snapshot races with concurrent producers/consumers;
under heavy concurrency the threshold can be exceeded momentarily before the
next admission check fires. This is acceptable — the threshold is a soft
signal, not a hard limit; the hard backstop is the channel-full `default`
branch.

#### 3. Split channel buffer sizing

Today (`pipeline.go:11-18`):

```go
batches = make(chan *batch, config.BatchCapacity)
work1   = make(chan *unitOfWork, config.BatchCapacity)
// ... work2..work5 same
```

Proposed:

```go
batches = make(chan *batch, config.BatchCapacity)
work1   = make(chan *unitOfWork, config.UnitCapacity)
work2   = make(chan *unitOfWork, config.UnitCapacity)
work3   = make(chan *unitOfWork, config.UnitCapacity)
work4   = make(chan *unitOfWork, config.UnitCapacity)
work5   = make(chan *unitOfWork, config.UnitCapacity)
```

And in `fanout.go:17`, the per-worker output channels (currently hardcoded
to 1024) become `make(chan *unitOfWork, unitCapacity)` where `unitCapacity` is
threaded through `newFanOut`'s signature (or via the existing
`stationFactory` closure — to be decided at implementation time, lower-blast-radius
option preferred).

`UnitCapacity` defaults to 1. Tunable via `Options.UnitCapacity(value int)`.
Setting it equal to `BatchCapacity` reproduces today's behavior.

**Why default 1, not 0?** Fully unbuffered channels turn every stage handoff
into a synchronization barrier — stage N can't begin unit N+1 until stage N+1
has received unit N. Buffer-1 lets stage N finish unit N+1 *while* stage N+1
is processing unit N. Pipelining benefit saturates at depth ~1 since each
stage runs in a single goroutine (except serialization, which has its own
fan-out concurrency).

**Bound on in-memory drift during an outage** — with `UnitCapacity=1` and
5 channels post-domain, plus the in-flight unit at each stage, the worst case
is ~10 units' worth of unpersisted mutations. At `UnitSize=64` that's
~640 batches' worth of broadcast results downstream of Execution.

The single-message `HandleResult` signature *also* tightens the upstream
side: each HTTP-admitted batch on the `batches` channel now carries exactly
one input message rather than potentially many, so `BatchCapacity` becomes a
direct count of in-flight HTTP commands rather than a count of caller
invocations of arbitrary fan-out. Combined with the `UnitCapacity=1`
post-domain default, the total worst-case in-memory drift is meaningfully
smaller than the previous draft's estimate, while throughput pipelining is
preserved.

### Non-goals

- **Rewriting the pipeline.** The structure (Entrypoint → Execution →
  Serialization → Persistence → Completion → Broadcast → Terminal) is
  preserved verbatim.
- **Changing `messaging.Handler`.** `Handle(ctx, messages ...any)` keeps its
  exact existing contract. `HandleResult` is a new method, not a replacement,
  and intentionally has a different signature (single message, returning an
  outcome).
- **Domain-layer changes.** The companion `AdjustOrder` idempotency change is
  in another repo and is assumed merged before any consumer relies on
  `OutcomeCallerDeparted`. Nothing in this proposal touches domain code.
- **Wireup/HTTP shell changes.** Phase 2 and Phase 4 of the parent proposal
  live in `billing-context`. Out of scope here.
- **`harness/sqladapter` changes.** No code changes; just a regression check
  via `go test`.

### Files modified (this repo only)

| Path                                     | Action | Purpose                                                                                                                               |
|------------------------------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------|
| `handlers/harness/00_entrypoint.go`      | Modify | Split into two paths: `Handle` (MQ/cron, blocking, no shed, variadic) and `HandleResult` (HTTP, ctx-aware, shed-aware, single message); shared helpers |
| `handlers/harness/contracts.go`          | Modify | New `HandleOutcome` enum; new `LoadShed`, `CallerDeparted` event types and unexported sentinel values                                 |
| `handlers/harness/config.go`             | Modify | `Options.UnitCapacity(int)`, `Options.ShedThreshold(float64)`; defaults 1, 0.80                                                       |
| `handlers/harness/pipeline.go`           | Modify | Use `UnitCapacity` for `work1`–`work5`; pass it into `newFanOut`; thread `ShedThreshold` into `newEntrypoint`                         |
| `handlers/harness/fanout.go`             | Modify | Accept `unitCapacity` and use it for the per-worker output channels instead of the hardcoded 1024                                     |
| `handlers/harness/00_entrypoint_test.go` | Modify | New tests for context-aware return, shed behavior, outcome reporting (single-message API), plus pinning tests for the existing `Handle` contract |
| `handlers/harness/config_test.go`        | Modify | Assert defaults for `UnitCapacity`, `ShedThreshold`; assert override setters                                                          |
| `handlers/harness/pipeline_test.go`      | Modify | Adjust assertions if any depend on default channel sizes (none expected to break — defaults preserve external observable behavior)    |
| `doc/work-sessions/2026/2026-05-14_pipeline-component-diagram.svg` | Modify | Reflect split `BatchCapacity`/`UnitCapacity` knobs, `LoadShed`/`CallerDeparted` Monitor observations, and `HandleOutcome` return path |

### Alternatives considered

- **Keep `HandleResult` variadic to mirror `Handle`.** Rejected — every HTTP
  route invokes the domain with exactly one command, so a variadic signature
  would invite empty-call and multi-call bugs at the boundary, and would
  loosen the WIP bound that `UnitCapacity=1` is meant to enforce. A
  single-`message any` parameter is both ergonomic at the HTTP call site and
  load-bearing for the resilience story.
- **Single shared method that branches on caller type via a `ctx` value or
  `Options.Source`.** Rejected — `streaming` acks unconditionally on clean
  `Handle` return, so an MQ-side shed-then-return would silently drop
  messages. The two paths require fundamentally different behavior, not
  different parameter values. Two methods make the contract visible at every
  call site.
- **Drop the `default` arm in `HandleResult`'s `select` and rely solely on
  high-watermark shedding.** Rejected — keep `default` as a hard backstop in
  case `shedThreshold` is misconfigured.
- **Apply shed to MQ as well, and panic on shed so the broker redelivers
  after reconnect.** Rejected — noisy, tangles error handling, and produces
  alarming log output in normal load-shed conditions. The blocking-channel-send
  back-pressure path through MQ prefetch is the standard RabbitMQ flow-control
  mechanism and works without instrumentation changes.
- **Add nack/error return to `messaging.Handler.Handle` itself.** Rejected as
  out-of-scope — would touch every existing `messaging.Handler` implementation
  across all consumers. A future messaging-library-level change could add
  this; this proposal does not.
- **Inject a per-batch `ctx` through Persistence and Broadcast.** Rejected.
  Per-batch ctx in retry-forever stages would unwind partially-completed work
  and break the durability principle. The pipeline ctx (`harness.New(ctx, …)`)
  is the right scope for those stages.
- **Keep `BatchCapacity` sizing all channels uniformly.** Rejected — preserves
  the in-memory drift problem during outages.

## Trade-offs & Risks

- **`HandleResult` exposes a per-call result outside the `messaging.Handler`
  abstraction.** Acceptable because the harness already exports `Monitor`,
  `Writer`, `Dispatcher`, etc. — the package is explicitly the "single
  ingress" abstraction. The new `HandleOutcome` type and the new method live
  on `*entrypoint` and can be retrieved via type assertion or via a
  small interface that the consumer defines.
- **Single-message HTTP signature is a hard constraint.** A hypothetical
  future HTTP route that needed to submit multiple commands atomically would
  not fit. Acceptable today — every existing HTTP route in `billing-context`
  invokes the domain with exactly one command — and reversible later if
  needed (the signature can grow back to variadic without breaking existing
  call sites by switching to a slice or by introducing a sibling
  `HandleResultBatch` method).
- **`waiterDone` allocates a goroutine and a channel per `HandleResult` call.**
  This is on the HTTP path only and only when the call actually waits (i.e. is
  not shed). The cost is a few hundred bytes and one goroutine for the
  duration of the in-flight batch — well within the budget of an HTTP request.
- **`UnitCapacity=1` reduces normal-throughput headroom slightly.** Pipelining
  is preserved (depth-1 buffer between stages) but bursty workloads that
  previously absorbed into deep buffers will now apply backpressure earlier.
  Mitigation: configurable; a deployment that prefers the old throughput
  characteristic can set `Options.UnitCapacity(1024)`.
- **Caller-departed batches keep doing work the caller no longer cares about.**
  Intentional — matches the durability principle. Combined with the merged
  domain-layer idempotency change (in `billing-context`), repeated retries
  collapse to no-ops after the first applies. From this module's perspective
  this is purely a contract guarantee: "we will not unwind the in-flight
  batch when the caller departs."
- **The shed-threshold as a fraction is inexact.** `len(chan)/cap(chan)` is
  a snapshot that races with concurrent producers/consumers; under heavy
  concurrency we can exceed the threshold momentarily before the next
  admission check fires. Acceptable — soft signal, not a hard limit; hard
  backstop is the channel-full `default` branch.
- **`Handle` and `HandleResult` share state (`this.work`, `this.lock`,
  `this.closed`).** Two paths writing to the same channel under the same
  RWMutex is fine; race-free under `-race`. Tests must cover the case where
  both paths interleave on a shrunk-`BatchCapacity` fixture.
- **Cross-repo coordination.** This module's changes are backward-compatible
  (new options have defaults; new method doesn't break existing
  `messaging.Handler` callers). A consumer that doesn't yet adopt
  `HandleResult` keeps working unchanged. The consuming repo's adoption is
  sequenced after a tagged release.
- **Diagram drift.** The pipeline diagram is the canonical visual reference;
  if the SVG isn't updated alongside the code, reviewers will form a stale
  mental model. Mitigation: diagram update is in the checklist.

## Implementation Checklist

### Phase 1: Configuration plumbing (red/green)

- [ ] Edit `handlers/harness/config_test.go` (`TestDefaultsPopulateCapacities`) to also assert `cfg.UnitCapacity == 1` and `cfg.ShedThreshold == 0.80`. Run `make test` — confirm failure (fields don't exist yet → compile error).
- [ ] Edit `handlers/harness/config_test.go` (`TestTunableOptionsOverrideDefaults`) to also exercise `Options.UnitCapacity(2)` and `Options.ShedThreshold(0.5)` and assert the values stick. Compile error still expected.
- [ ] Edit `handlers/harness/config.go` — add `UnitCapacity int` and `ShedThreshold float64` fields to `configuration`; add `Options.UnitCapacity(int)` and `Options.ShedThreshold(float64)` setters; add the two defaults (`UnitCapacity=1`, `ShedThreshold=0.80`) to `Options.defaults(...)`.
- [ ] Run `make test` — confirm config tests pass.

### Phase 2: Pipeline rewires for split capacity (red/green)

- [ ] Edit `handlers/harness/pipeline.go` — change `work1`–`work5` to `make(chan *unitOfWork, config.UnitCapacity)`; thread `config.UnitCapacity` into the `newFanOut` call.
- [ ] Edit `handlers/harness/fanout.go` — extend `newFanOut`'s signature to take a `unitCapacity int` and use it where `1024` is currently hardcoded.
- [ ] Run `make test` — pipeline tests should still pass under the new defaults; if any test depends on the old 1024 buffer it should be updated to set `Options.UnitCapacity(1024)` explicitly.

### Phase 3: Monitor observations and outcome enum

- [ ] Edit `handlers/harness/contracts.go` — add `HandleOutcome` type (`int`-backed enum: `OutcomeAccepted=0`, `OutcomeShed=1`, `OutcomeCallerDeparted=2`); add `LoadShed struct{}` and `CallerDeparted struct{}` event types alongside the existing `BatchInFlight`/`BatchComplete`/etc.
- [ ] Edit `handlers/harness/00_entrypoint.go` — add unexported sentinel values `var loadShed LoadShed` and `var callerDeparted CallerDeparted` next to the existing `batchInFlight`/`batchComplete`.
- [ ] Run `make test` — confirm the existing suite still compiles and passes.

### Phase 4: Extract shared helpers (pure refactor — keep `Handle` behavior identical)

- [ ] Refactor `handlers/harness/00_entrypoint.go` to extract `prepare(ctx, messages ...any) (*sync.WaitGroup, *batch)`, `abandon(waiter, item)`, and `waiterDone(waiter) chan struct{}` helpers; rewrite `Handle`'s body in terms of `prepare(ctx, messages...)` and the existing admission logic so it is observably identical.
- [ ] Run `make test` — all existing tests must still pass; this step changes no externally observable behavior.

### Phase 5: Add `HandleResult` (TDD, HTTP path, single-message signature)

- [ ] In `handlers/harness/00_entrypoint_test.go`, add `TestHandleResult_OutcomeAcceptedOnSuccess` — call `HandleResult(ctx, "msg")` with a single message; let the pipeline complete; assert outcome is `OutcomeAccepted`. Run `make test` — confirm failure (no `HandleResult` method yet → compile error).
- [ ] Add `HandleResult(ctx context.Context, message any) HandleOutcome` method on `*entrypoint` with the body shown in §1 of Approach. The method calls `this.prepare(ctx, message)` (Go promotes the single argument into the helper's variadic slot). Note: the entrypoint must hold its `shedThreshold` field — wire it through `newEntrypoint` from `pipeline.go`. Run `make test` — confirm `OutcomeAcceptedOnSuccess` passes.
- [ ] Add `TestHandleResult_OutcomeCallerDepartedOnContextCancel` — fixture with a writer that blocks forever; cancel the caller's `ctx`; assert outcome is `OutcomeCallerDeparted` and Monitor sees `CallerDeparted{}`. Run — confirm passing.
- [ ] Add `TestHandleResult_OutcomeShedAtHighWatermark` — fixture with `BatchCapacity=10`, `ShedThreshold=0.5`, writer that blocks forever; submit 5 messages via `HandleResult` (each call carries one message); assert the 6th-and-beyond return `OutcomeShed` and Monitor sees `LoadShed{}`. Run — confirm passing.
- [ ] Add `TestHandleResult_OutcomeShedOnHardFull` — `ShedThreshold=2.0` (high-watermark disabled); writer blocks; submit `BatchCapacity+1` messages via `HandleResult` (one per call); assert the overflowing one returns `OutcomeShed`. Run — confirm passing.
- [ ] Add `TestHandleResult_OutcomeShedReleasesPoolEntry` — submit a message that gets shed via `HandleResult`; assert the entrypoint's `batches` pool has the same length before and after (no leak). Run — confirm passing.
- [ ] Add `TestHandleResult_ClosedPipelineReturnsShed` — close the entrypoint; call `HandleResult(ctx, "msg")`; assert outcome is `OutcomeShed` and the pool entry is returned. Run — confirm passing.
- [ ] Add `TestHandleResult_BatchCarriesExactlyOneMessage` — `HandleResult(ctx, "only")`; intercept the resulting `*batch` on the work channel; assert `len(item.messages) == 1` and `item.messages[0] == "only"`. Run — confirm passing (this pins the single-message contract).

### Phase 6: Pin the existing `Handle` contract (TDD, MQ/cron path)

- [ ] Add `TestHandle_BlocksUntilDurable` — submit a batch via `Handle`; the writer takes a controlled delay to acknowledge; assert `Handle` returns only after the writer completes. Run — confirm passing (this pins the contract `streaming` depends on).
- [ ] Add `TestHandle_DoesNotShedAtHighWatermark` — fixture with `BatchCapacity=2`, `ShedThreshold=0.5`, writer that blocks forever; submit 5 batches via `Handle` (each in its own goroutine); assert all 5 are blocked (none returned). After unblocking the writer, all 5 should eventually return. Run — confirm passing.
- [ ] Add `TestHandle_DoesNotShedOnHardFull` — `BatchCapacity=2`; submit 3 batches via `Handle` from separate goroutines; assert the third blocks on the channel send rather than returning early. Run — confirm passing.
- [ ] Add `TestHandle_IgnoresContextCancel` — submit a batch via `Handle`; cancel the ctx; assert `Handle` is still blocked until the pipeline completes the batch. Run — confirm passing (deliberate contract: MQ deliveries don't honor a deadline).
- [ ] Add `TestHandle_ReturnsImmediatelyOnClosedPipeline` — close the entrypoint; call `Handle`; assert it returns within a few milliseconds (no panic, no block). Run — confirm passing (this is today's behavior, just being pinned).
- [ ] Add `TestHandle_PreservesVariadicMessages` — `Handle(ctx, "a", "b", "c")`; intercept the resulting `*batch`; assert `len(item.messages) == 3`. Run — confirm passing (pins that the variadic contract still works after the `prepare` refactor).

### Phase 7: Race and integration sanity

- [ ] Run the full harness test suite under `-race`: `go test -race ./handlers/harness/...`. Confirm green.
- [ ] Run `make test` (the project-level entry point that also runs `go mod tidy`, `go fmt ./...`). Confirm green.
- [ ] Run `go test ./handlers/harness/sqladapter/...` against a live MySQL (drop `-short` if present, or use the project's `make test.db.local`-equivalent). Confirm no regressions in the SQL adapter.

### Phase 8: Documentation

- [ ] Update `doc/work-sessions/2026/2026-05-14_pipeline-component-diagram.svg` to reflect: split `BatchCapacity`/`UnitCapacity` knobs, `LoadShed`/`CallerDeparted` Monitor observations, and the `HandleResult(ctx, message any) → HandleOutcome` return path. (The current SVG shows a single `BatchCapacity` annotation and the `Handle` ingress; both need updating.)
- [ ] Self-review the diff: confirm no `messaging.Handler` callers were inadvertently broken; confirm `Options.UnitCapacity(1024)` (the old default) reproduces today's runtime if a user wants it; confirm no domain code or sqladapter code was touched.

### Out of scope (handled in `billing-context` follow-on)

- HTTP shell wireup, error definitions, integration tests, `mapOutcome` helper, application-side monitor metric registration, post-deploy observation drills. These will be addressed in a separate proposal/session in the consuming repo after this module's changes merge and a version is tagged.
