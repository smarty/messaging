---
name: Harness pipeline resilience (module-local changes)
description: Implement the messaging/v3 harness side of the cross-repo "harness resilience and idempotency" proposal — a void context-honoring HTTP entrypoint (unexported await) alongside today's Handle, a pre-flight admission gate (unexported admit) plus an in-module net/http shedding middleware, a thin AsHTTPHandler decorator so HTTP shells need no changes, and split channel-buffer sizing (BatchCapacity vs UnitCapacity). Excludes per-service route wireup and post-deploy observation, which belong to the consuming repos.
type: plot
---

# Proposal: Harness Pipeline Resilience — Module-Local Changes

## Background

The cross-repo proposal at
`billing-context/.../2026-05-15_23-02-32-proposal-harness-resilience-and-idempotency.md`
describes three operational changes layered on top of Chunk C of the incremental
domain transformation. The bulk of those changes live entirely inside this
module (`github.com/smarty/messaging/v3`); the only per-consumer work that
remains is the mechanical wrapping of each mutating route, which lives in the
~10 `*-context` services and is out of scope here.

This proposal scopes the work to just the parts that land in *this* repo:

- All edits under `handlers/harness/` (entrypoint, contracts, config, pipeline,
  fanout, the new admission middleware + decorator, and their tests).
- A documentation update to `doc/work-sessions/2026/2026-05-14_pipeline-component-diagram.svg`
  to reflect the split capacity knobs, the admission gate, and the new monitor
  observations.

The per-service route wireup (calling `AsHTTPHandler` and `Admission` from each
service's routes table), integration tests, and post-deploy observation steps
from the parent proposal happen in each consuming repo *after* this repo's
changes merge and a tagged version of `messaging/v3` is published. They are
explicitly **out of scope** for this proposal.

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

### Why a *void* HTTP entrypoint (the central design constraint)

An earlier draft gave the HTTP entrypoint the signature
`HandleResult(ctx, message any) HandleOutcome`, returning an enum the caller
mapped to `503`/`504`. That return value would force **every** mutating HTTP
route to branch on the outcome — and to add the corresponding test cases to
maintain coverage. Across the ~10 `*-context` services there are **70+
mutating routes**; replicating outcome-mapping logic (and its tests) at each
one is exactly the kind of accumulated, duplicated branching we want to avoid.

Two observations dissolve the need for a return value:

- **Shedding is a *pre-flight* decision, not a *result*.** It can be decided
  before the route handler ever runs. A small `admit() bool` predicate plus a
  single HTTP middleware (written once, in this module) writes the `503` and
  short-circuits the route. The route handler — and its tests — never see it.
- **A departed caller does not need a status override.** When the caller's
  `ctx` fires mid-flight, the goroutine simply unblocks (fixing the leak in
  concern #1) and the route writes its normal response to a client that is
  already gone. No `504`, no buffering of the response, no extra machinery. The
  in-flight batch keeps processing and durably stores regardless.

That leaves the HTTP entrypoint **void**, identical in spirit to today's
`Handle`. The harness-side fix has these independently-mergeable pieces:

1. **A void, context-honoring HTTP entrypoint** — an unexported
   `await(ctx, message any)` alongside the existing
   `Handle(ctx, messages ...any)`. `await` honors `ctx.Done()`, processes
   exactly one message, emits a `CallerDeparted` observation when the caller
   leaves, and returns nothing.
2. **Pre-flight admission** — an unexported `admit() bool` predicate on the
   entrypoint (high-watermark check against the `batches` channel, used only by
   the in-package middleware) and an in-module `Admission` HTTP middleware that
   writes a `503` (inline, raw `net/http`) when the gate refuses. A thin
   `AsHTTPHandler` decorator adapts `await` to the `messaging.Handler` interface
   the HTTP shells already depend on, so neither the shells nor their tests
   change.
3. **Split channel-buffer sizing** — `BatchCapacity` continues to size the
   caller-side `batches` channel; a new `UnitCapacity` (default 1) sizes
   `work1`–`work5` and the per-worker fan-out outputs.

The companion `AdjustOrder` domain idempotency change (separate proposal,
already merged in the consuming repos) makes it safe for `await` to return
early when the caller's `ctx` fires: the in-flight batch keeps processing and
durably stores, and a client retry collapses to a no-op once the original batch
has persisted.

## Approach

### Decision summary

Three methods on `*entrypoint` (two of them unexported), plus two thin
module-local adapters that are the *only* new exported surface:

| Method                              | Visibility | Caller               | Honors `ctx.Done()` | Sheds   | Return | Arity       |
|-------------------------------------|------------|----------------------|---------------------|---------|--------|-------------|
| `Handle(ctx, messages ...any)`      | exported   | MQ, cron             | No                  | No      | none   | variadic    |
| `await(ctx, message any)`           | in-package | HTTP (via adapter)   | Yes                 | No      | none   | exactly one |
| `admit() bool`                      | in-package | HTTP middleware      | n/a                 | Decides | `bool` | n/a         |

| Adapter (this module, exported)                            | Role                                                                                       |
|------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| `AsHTTPHandler(messaging.Handler) messaging.Handler`       | Wraps the handler's `await` so HTTP shells keep depending on `messaging.Handler` (zero shell change) |
| `Admission(messaging.Handler, http.Handler) http.Handler`  | Pre-flight gate; writes inline `503` when `admit()` is false                               |

`await`, `admit`, and the `awaiter`/`admitter` interfaces the adapters assert
against are all **unexported**. Because both the middleware and the decorator
live in this package, the consumer never names them — `AsHTTPHandler(handler)`
and `Admission(handler, shell)` are the entire integration vocabulary, and both
take/return the standard `messaging.Handler`/`http.Handler` types.

`await` takes a single `message any` (not variadic) because every HTTP route in
every consuming service invokes the domain with exactly one command per
request. Constraining the signature here:

- Eliminates the empty-slice / multi-message edge cases on the HTTP path.
- Tightens the worst-case in-memory work-in-progress bound: combined with
  `UnitCapacity=1`, each in-flight HTTP request contributes exactly one input
  message to a batch. The `batches` channel capacity now corresponds directly
  to a count of HTTP commands enqueued rather than a count of caller
  invocations of arbitrary size.
- Surfaces the asymmetry plainly — MQ/cron may legitimately deliver multiple
  events per call (broker batch deliveries); HTTP does not.

Two new monitor observations:

- `LoadShed{}` — emitted by `admit()` when it refuses on the high-watermark
  check. (Refusal because the pipeline is `closed` is shutdown, not load, and
  emits nothing.)
- `CallerDeparted{}` — emitted by `await` when the caller's `ctx` fired before
  completion (whether during enqueue or during the wait).

Two new configuration options with defaults:

- `Options.UnitCapacity(int)` — default `1`. Sizes `work1`–`work5` and the
  per-worker fan-out outputs.
- `Options.ShedThreshold(float64)` — default `0.80`. Fraction of the `batches`
  channel capacity at or past which `admit()` refuses.

### Detailed design

#### 1. Shared helpers (pure refactor of today's `Handle`)

Today's `00_entrypoint.go:29` has a single `Handle` whose body inlines waiter
acquisition, batch allocation, completion-callback wiring, the
admission-under-RWMutex sequence, and the wait. The split extracts three
private helpers shared by `Handle` and `await`. `prepare` keeps its variadic
`...any` shape so `Handle` passes its argument through verbatim; `await` calls
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

#### 2. Path A — `Handle` (MQ and cron): preserves today's contract verbatim

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
  `streaming` to ack work the pipeline never finished.
- **No load-shed.** Sending to `this.work` is a blocking channel send.
  Back-pressure propagates to the broker via prefetch limits and unacked
  counts.
- **The only "shed" condition is pipeline shutdown** (`this.closed`) — exactly
  today's behavior.

#### 3. Path B — `await` (HTTP): void, context-honoring, single message

```go
func (this *entrypoint) await(ctx context.Context, message any) {
	waiter, item := this.prepare(ctx, message)
	defer this.waiters.Put(waiter)

	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		this.abandon(waiter, item)
		return
	}
	select {
	case this.work <- item:
		this.monitor.Track(batchInFlight)
		this.lock.RUnlock()
	case <-ctx.Done():
		this.lock.RUnlock()
		this.abandon(waiter, item)
		this.monitor.Track(callerDeparted)
		return
	}

	select {
	case <-this.waiterDone(waiter):
	case <-ctx.Done():
		this.monitor.Track(callerDeparted)
	}
}
```

Properties:
- **Void.** No outcome to return — shedding is handled pre-flight by the
  middleware (see §4), and a departed caller just unblocks.
- **Single message per call.**
- **Honors `ctx.Done()` in both the enqueue and the wait.** This is what fixes
  the indefinite-stacking concern: an HTTP request whose client deadline passes
  (or whose load balancer cancels) unblocks promptly instead of parking.
- **No hard-full backstop.** The ctx-honoring send already bounds the enqueue
  wait, so there is no need for a non-blocking `default` arm — and dropping it
  avoids a silent post-admit shed that would leave `command.Result` zero and
  cause the shell to emit a wrong status (e.g. 404). `admit()` is the watermark
  gate; the ctx-honoring send is the backstop.

**Pool-entry lifecycle:**
- Success / wait-departed path: the batch was enqueued, so the pipeline owns it
  and will invoke `item.complete()` (which `Put`s it). `await` must **not**
  `Put` — the pool would receive the same item twice. (On wait-departure the
  pipeline still completes the batch; only the HTTP goroutine returns early.)
- Enqueue-departed and closed paths: the item was never enqueued, so
  `complete()` will never fire; `abandon(waiter, item)` does the cleanup.

**Note — why `complete()` owns the `Put`, not the entrypoint.** A tempting
simplification is to have `complete()` only release the waiter and let the
entrypoint `Put` the batch once its own wait returns. That works for `Handle`
(which always waits for completion) but is *incorrect* for `await`: on the
wait-departed path `await` returns on `ctx.Done()` **before** completion, so an
entrypoint-side `Put` would never run and the pooled `*batch` would leak (worse,
the pipeline's later `complete()` would mutate an item the entrypoint believed
it had reclaimed). Completion-owned `Put` is precisely what lets a single
cleanup rule cover both "caller waited" and "caller departed but the pipeline
finished later." The entrypoint only `Put`s — via `abandon` — on the paths
where the batch was *never* enqueued and `complete()` will therefore never fire.
So the current split isn't just cleaner; it's the only correct allocation of
the `Put`.

**Critically, the batch is not abandoned by the pipeline when the caller
departs.** When `ctx` fires after enqueue, the in-flight batch keeps
processing; `complete()` still fires; persistence still happens. Only the HTTP
caller's goroutine returns early.

#### 4. Pre-flight admission: `admit()` + `Admission` middleware + `AsHTTPHandler`

The high-watermark check lives in a side-effect-light predicate on the
entrypoint:

```go
func (this *entrypoint) admit() bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	if this.closed {
		return false
	}
	if float64(len(this.work))/float64(cap(this.work)) >= this.shedThreshold {
		this.monitor.Track(loadShed)
		return false
	}
	return true
}
```

`Options.ShedThreshold(value float64)` exposes the threshold; default `0.80`.
Setting it ≥ `1.0` disables high-watermark shedding (only the closed-pipeline
refusal remains).

The middleware and the decorator live in this module (new file
`handlers/harness/admission.go`) and depend only on the standard library and
`messaging/v3`. The `awaiter`/`admitter` interfaces are unexported; the adapters
accept the standard `messaging.Handler` returned by `New(...)` and assert it to
those interfaces internally (a failed assertion is a wireup-time programming
error and panics fast). The `503` response is flushed inline with raw
`net/http`:

```go
type (
	admitter interface {
		admit() bool
	}
	awaiter interface {
		await(ctx context.Context, message any)
	}
)

// Admission refuses overloaded requests before the wrapped handler runs,
// writing an inline 503. Wrap each mutating route with it.
func Admission(handler messaging.Handler, inner http.Handler) http.Handler {
	gate := handler.(admitter)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if gate.admit() {
			inner.ServeHTTP(response, request)
			return
		}
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		response.Header().Set("Retry-After", "1")
		response.WriteHeader(http.StatusServiceUnavailable)
		_, _ = response.Write(shedResponseBody)
	})
}

// AsHTTPHandler adapts the void, context-honoring await to the
// messaging.Handler the HTTP shells already depend on, so no shell (and no
// shell test) changes.
func AsHTTPHandler(handler messaging.Handler) messaging.Handler {
	return &httpAdapter{target: handler.(awaiter)}
}

type httpAdapter struct {
	target awaiter
}

func (this *httpAdapter) Handle(ctx context.Context, messages ...any) {
	for _, message := range messages {
		this.target.await(ctx, message)
	}
}

var shedResponseBody = []byte(`{"errors":[{"message":"service overloaded"}]}`)
```

**Why this keeps the route handlers (and their tests) untouched.** Each
consuming service's HTTP shells already build a command, call
`handler.Handle(ctx, command)` on a `messaging.Handler`, and map the *mutated
command's* result field (`command.Result`) to its response — they read no
return value from `Handle` today. Two seams preserve that exactly:

- `AsHTTPHandler(handler)` is substituted for the raw handler when the write
  shells are constructed, so `Handle` now routes through `await` (ctx-honoring,
  single-message) without the shell knowing.
- `Admission(handler, shell)` wraps each mutating route in the routes table, so
  the `503` is decided before the shell runs.

Illustrative consumer wireup (out of scope, shown for context only):

```go
handler, listeners := harness.New(ctx, opts...)
httpHandler := harness.AsHTTPHandler(handler)
// ...
{"POST   /admin/orders", harness.Admission(handler, NewAdminApproveOrderShell(httpHandler))},
{"PUT    /admin/accounts/:account/orders/:order/adjustments", harness.Admission(handler, NewAdminAdjustOrderShell(httpHandler))},
```

The consumer performs no type assertions of its own — it passes the
`messaging.Handler` from `New(...)` straight into both adapters. Each of the 70+
mutating routes across the ~10 services becomes a mechanical one-line wrap — no
per-route outcome logic, no per-route tests. The `503`/departed behavior is
tested **once**, here, against the middleware and the decorator.

**Race note.** `admit()`'s `len(chan)/cap(chan)` snapshot races with concurrent
producers/consumers, and there is a TOCTOU window between `admit()` returning
true and `await`'s enqueue. Both are acceptable: the threshold is a soft signal,
and the ctx-honoring send means that even if the channel fills in the race
window, the request either drains normally or unblocks on `ctx.Done()` — it
never produces a wrong status the way a silent post-admit shed would.

#### 5. Split channel buffer sizing

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

And in `fanout.go:17`, the per-worker output channels (currently hardcoded to
1024) become `make(chan *unitOfWork, unitCapacity)` where `unitCapacity` is
threaded through `newFanOut`'s signature (lower-blast-radius option preferred
at implementation time).

`UnitCapacity` defaults to 1. Tunable via `Options.UnitCapacity(value int)`.
Setting it equal to `BatchCapacity` reproduces today's behavior.

**Why default 1, not 0?** Fully unbuffered channels turn every stage handoff
into a synchronization barrier — stage N can't begin unit N+1 until stage N+1
has received unit N. Buffer-1 lets stage N finish unit N+1 *while* stage N+1
is processing unit N. Pipelining benefit saturates at depth ~1 since each
stage runs in a single goroutine (except serialization, which has its own
fan-out concurrency).

**Bound on in-memory drift during an outage** — with `UnitCapacity=1` and 5
channels post-domain, plus the in-flight unit at each stage, the worst case is
~10 units' worth of unpersisted mutations. At `UnitSize=64` that's ~640
batches' worth of broadcast results downstream of Execution.

The single-message `await` signature *also* tightens the upstream side: each
HTTP-admitted batch on the `batches` channel now carries exactly one input
message, so `BatchCapacity` becomes a direct count of in-flight HTTP commands
rather than a count of caller invocations of arbitrary fan-out.

### Non-goals

- **Rewriting the pipeline.** The structure (Entrypoint → Execution →
  Serialization → Persistence → Completion → Broadcast → Terminal) is
  preserved verbatim.
- **Changing `messaging.Handler`.** `Handle(ctx, messages ...any)` keeps its
  exact existing contract. `await` is a new (unexported) method, not a
  replacement, and intentionally has a different signature (single message,
  void).
- **Returning a status outcome from the HTTP path.** Explicitly rejected — see
  Background and Alternatives. Shedding is decided pre-flight; departed callers
  just unblock.
- **Buffering the HTTP response to override status post-hoc.** Not done — a
  departed caller is already gone, so the shell's normal write to a dead
  connection is harmless, and no `504` is emitted.
- **Per-service route wireup.** Calling `AsHTTPHandler` / `Admission` from each
  service's routes table happens in the consuming repos, after a tagged
  release. Out of scope here.
- **Domain-layer changes.** The companion `AdjustOrder` idempotency change is
  in the consuming repos and is assumed merged before any consumer relies on a
  departed-caller batch persisting. Nothing in this proposal touches domain
  code.
- **`harness/sqladapter` changes.** No code changes; just a regression check
  via `go test`.

### Files modified (this repo only)

| Path                                                               | Action | Purpose                                                                                                                                                                                                 |
|--------------------------------------------------------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `handlers/harness/00_entrypoint.go`                                | Modify | Extract `prepare`/`abandon`/`waiterDone`; keep `Handle` behavior identical; add unexported `await(ctx, message any)` and `admit() bool`; add `shedThreshold` field; add `loadShed`/`callerDeparted` sentinels |
| `handlers/harness/admission.go`                                    | Add    | Unexported `admitter`/`awaiter` interfaces; `Admission(messaging.Handler, http.Handler) http.Handler` (inline `503`); `AsHTTPHandler(messaging.Handler) messaging.Handler` decorator; `shedResponseBody` |
| `handlers/harness/contracts.go`                                    | Modify | New `LoadShed` and `CallerDeparted` event types alongside the existing `BatchInFlight`/`BatchComplete`/etc.                                                                                             |
| `handlers/harness/config.go`                                       | Modify | `Options.UnitCapacity(int)`, `Options.ShedThreshold(float64)`; defaults 1, 0.80                                                                                                                         |
| `handlers/harness/pipeline.go`                                     | Modify | Use `UnitCapacity` for `work1`–`work5`; pass it into `newFanOut`; thread `ShedThreshold` into `newEntrypoint`                                                                                           |
| `handlers/harness/fanout.go`                                       | Modify | Accept `unitCapacity` and use it for the per-worker output channels instead of the hardcoded 1024                                                                                                       |
| `handlers/harness/00_entrypoint_test.go`                           | Modify | New tests for `await` (void, ctx-honoring, single message, pool lifecycle) and `admit` (watermark/closed/threshold), plus pinning tests for `Handle`                                                    |
| `handlers/harness/admission_test.go`                               | Add    | Tests for `Admission` (pass-through when admitted; inline `503` body/headers when refused) and `AsHTTPHandler` (forwards to `await`)                                                                    |
| `handlers/harness/config_test.go`                                  | Modify | Assert defaults for `UnitCapacity`, `ShedThreshold`; assert override setters                                                                                                                            |
| `handlers/harness/pipeline_test.go`                                | Modify | Adjust assertions if any depend on default channel sizes (none expected to break — defaults preserve external observable behavior)                                                                      |
| `doc/work-sessions/2026/2026-05-14_pipeline-component-diagram.svg` | Modify | Reflect split `BatchCapacity`/`UnitCapacity` knobs, the `admit` gate + `Admission` middleware, `LoadShed`/`CallerDeparted` observations, and the void `await` ingress                                   |

### Alternatives considered

- **Return a `HandleOutcome` enum from the HTTP path and map it per-route.**
  Rejected — this is the motivating problem. Across ~10 `*-context` services
  and 70+ mutating routes, each route would replicate `503`/`504` branching and
  the tests to cover it. Centralizing the decision in a pre-flight gate +
  middleware keeps the route handlers void and tested once.
- **Buffer the HTTP `ResponseWriter` in middleware to override status after the
  shell runs (the "Option A" / `504` path).** Rejected — the consuming shells
  flush the response *inside* the handler, so a post-hoc override requires
  wrapping the writer in a buffering shim in this module. The only payoff would
  be a precise `504`, but a departed caller's connection is already gone, so the
  shell's normal write is harmless and no override is needed. More machinery, no
  real benefit.
- **Panic on shed + recover in middleware.** Rejected — shedding is a
  *high-frequency* condition precisely during an outage, so this would panic on
  a large fraction of requests and blur the "panic = bug = 500 + page someone"
  semantics that recovery middleware exists to serve. The parent proposal
  rejected panic-on-shed for the MQ path for the same noise reasons.
- **Put the shedding middleware in each consuming repo.** Rejected — 10
  services would duplicate the middleware and its tests. It lives once, here.
- **Export `await`/`admit` (or their interfaces) for the consumer to call.**
  Rejected — both are only ever invoked by the in-package `Admission`
  middleware and `AsHTTPHandler` decorator, so keeping them unexported shrinks
  the public surface to two functions and prevents consumers from reaching past
  the adapters. The adapters take/return standard `messaging.Handler` and assert
  to the unexported interfaces internally.
- **Keep a hard-full `default` backstop inside `await`.** Rejected — without a
  return value a silent post-admit shed would leave `command.Result` zero and
  the shell would emit a wrong status (e.g. 404). The ctx-honoring send bounds
  the enqueue wait without it; `admit()` is the watermark gate.
- **Single shared method branching on caller type via a `ctx` value or
  `Options.Source`.** Rejected — `streaming` acks unconditionally on clean
  `Handle` return, so an MQ-side shed-then-return would silently drop messages.
  The two paths require fundamentally different behavior. Separate methods make
  the contract visible at every wiring site.
- **Inject a per-batch `ctx` through Persistence and Broadcast.** Rejected.
  Per-batch ctx in retry-forever stages would unwind partially-completed work
  and break the durability principle. The pipeline ctx (`harness.New(ctx, …)`)
  is the right scope for those stages.
- **Keep `BatchCapacity` sizing all channels uniformly.** Rejected — preserves
  the in-memory drift problem during outages.
- **Add nack/error return to `messaging.Handler.Handle` itself.** Rejected as
  out-of-scope — would touch every existing `messaging.Handler` implementation
  across all consumers.

## Trade-offs & Risks

- **The only new exported surface is two functions — `AsHTTPHandler` and
  `Admission`.** Both take and return the standard `messaging.Handler` /
  `http.Handler` types. The `await`/`admit` methods and the `awaiter`/`admitter`
  interfaces they assert against are unexported, so the consumer never names
  them; `AsHTTPHandler(New(...))` and `Admission(New(...), shell)` are the whole
  integration vocabulary. The adapters type-assert the supplied handler to the
  unexported interfaces internally and panic at wireup if handed something other
  than the harness entrypoint — a deliberate fail-fast on misconfiguration.
- **This module now imports `net/http`** (in `admission.go`). Minor — it is a
  standard-library dependency, isolated to the admission file. If desired at
  implementation time, the middleware and decorator can move to a sibling
  subpackage (e.g. `handlers/harness/admission`) to keep `net/http` out of the
  core pipeline package; the unexported `awaiter`/`admitter` interfaces would
  then need to be exported (or the adapters constructed inside `harness` and
  re-exported). Lower-churn option (single package, unexported interfaces) is
  preferred unless review objects.
- **Single-message HTTP signature is a hard constraint.** A hypothetical future
  HTTP route needing to submit multiple commands atomically would not fit.
  Acceptable today — every existing HTTP route invokes the domain with exactly
  one command — and reversible later (a sibling `awaitBatch` could be added
  without breaking existing call sites).
- **`waiterDone` allocates a goroutine and a channel per `await` call.** On the
  HTTP path only. The cost is a few hundred bytes and one goroutine for the
  duration of the in-flight batch — well within an HTTP request's budget. On a
  wait-departure the goroutine parks until the pipeline eventually completes the
  batch, then exits.
- **`UnitCapacity=1` reduces normal-throughput headroom slightly.** Pipelining
  is preserved (depth-1 buffer between stages) but bursty workloads that
  previously absorbed into deep buffers now apply backpressure earlier.
  Mitigation: configurable; set `Options.UnitCapacity(1024)` to reproduce
  today's characteristic.
- **Caller-departed batches keep doing work the caller no longer cares about.**
  Intentional — matches the durability principle. Combined with the merged
  domain-layer idempotency change, repeated retries collapse to no-ops after
  the first applies. From this module's perspective this is a contract
  guarantee: "we will not unwind the in-flight batch when the caller departs."
- **The shed-threshold as a fraction is inexact, and there is a TOCTOU window
  between `admit()` and `await`'s enqueue.** Acceptable — soft signal, not a
  hard limit; and the ctx-honoring send means a race-window channel-full never
  produces a wrong status (it drains or unblocks on `ctx.Done()`).
- **`Handle` and `await` share state (`this.work`, `this.lock`, `this.closed`).**
  Two paths writing the same channel under the same RWMutex is fine; race-free
  under `-race`. Tests must cover both paths interleaving on a shrunk-capacity
  fixture.
- **Cross-repo coordination.** This module's changes are backward-compatible
  (new options have defaults; the new exported functions don't break existing
  `messaging.Handler` callers). A consumer that doesn't yet wrap its routes
  keeps working unchanged. Per-service adoption is sequenced after a tagged
  release.
- **Diagram drift.** The pipeline diagram is the canonical visual reference; if
  the SVG isn't updated alongside the code, reviewers will form a stale mental
  model. Mitigation: diagram update is in the checklist.

## Implementation Checklist

### Phase 1: Configuration plumbing (red/green)

- [x] Edit `handlers/harness/config_test.go` (`TestDefaultsPopulateCapacities`) to also assert `cfg.UnitCapacity == 1` and `cfg.ShedThreshold == 0.80`. Run `make test` — confirm failure (fields don't exist yet → compile error).
- [x] Edit `handlers/harness/config_test.go` (`TestTunableOptionsOverrideDefaults`) to also exercise `Options.UnitCapacity(2)` and `Options.ShedThreshold(0.5)` and assert the values stick. Compile error still expected.
- [x] Edit `handlers/harness/config.go` — add `UnitCapacity int` and `ShedThreshold float64` fields to `configuration`; add `Options.UnitCapacity(int)` and `Options.ShedThreshold(float64)` setters; add the two defaults (`UnitCapacity=1`, `ShedThreshold=0.80`) to `Options.defaults(...)`.
- [x] Run `make test` — confirm config tests pass.

### Phase 2: Pipeline rewires for split capacity (red/green)

- [x] Edit `handlers/harness/pipeline.go` — change `work1`–`work5` to `make(chan *unitOfWork, config.UnitCapacity)`; thread `config.UnitCapacity` into the `newFanOut` call.
- [x] Edit `handlers/harness/fanout.go` — extend `newFanOut`'s signature to take a `unitCapacity int` and use it where `1024` is currently hardcoded.
- [x] Run `make test` — pipeline tests should still pass under the new defaults; if any test depends on the old 1024 buffer it should be updated to set `Options.UnitCapacity(1024)` explicitly.

### Phase 3: Monitor observations and sentinels

- [x] Edit `handlers/harness/contracts.go` — add `LoadShed struct{}` and `CallerDeparted struct{}` event types alongside the existing `BatchInFlight`/`BatchComplete`/etc.
- [x] Edit `handlers/harness/00_entrypoint.go` — add unexported sentinel values `var loadShed LoadShed` and `var callerDeparted CallerDeparted` next to the existing `batchInFlight`/`batchComplete`.
- [x] Run `make test` — confirm the existing suite still compiles and passes.

### Phase 4: Extract shared helpers (pure refactor — keep `Handle` behavior identical)

- [x] Refactor `handlers/harness/00_entrypoint.go` to extract `prepare(ctx, messages ...any) (*sync.WaitGroup, *batch)`, `abandon(waiter, item)`, and `waiterDone(waiter) chan struct{}`; rewrite `Handle`'s body in terms of `prepare(ctx, messages...)` so it is observably identical.
- [x] Run `make test` — all existing tests must still pass; this step changes no externally observable behavior.

### Phase 5: Add `await` (TDD, void HTTP path, single message)

- [x] Add `TestAwait_ReturnsAfterCompletion` — call `await(ctx, "msg")` with a single message; let the pipeline complete; assert the call returns and the batch was processed. Run `make test` — confirm failure (no `await` method yet → compile error). Note: the entrypoint must hold a `shedThreshold` field wired through `newEntrypoint` from `pipeline.go`.
- [x] Add the unexported `await(ctx context.Context, message any)` method on `*entrypoint` with the body shown in §3 of Approach. Run — confirm `ReturnsAfterCompletion` passes.
- [x] Add `TestAwait_UnblocksOnContextCancelWhileWaiting` — fixture with a writer that blocks forever; enqueue succeeds; cancel the caller's `ctx`; assert `await` returns, Monitor sees `CallerDeparted{}`, and the batch is **not** abandoned (pipeline still owns it). Run — confirm passing.
- [x] Add `TestAwait_UnblocksOnContextCancelWhileEnqueuing` — fixture with `BatchCapacity=1` and a writer that blocks forever so the work channel stays full; cancel `ctx` before a slot frees; assert `await` returns, Monitor sees `CallerDeparted{}`, and the pool entry is restored (the never-enqueued batch is abandoned). Run — confirm passing.
- [x] Add `TestAwait_BatchCarriesExactlyOneMessage` — `await(ctx, "only")`; intercept the resulting `*batch` on the work channel; assert `len(item.messages) == 1` and `item.messages[0] == "only"`. Run — confirm passing (pins the single-message contract).
- [x] Add `TestAwait_ClosedPipelineReturnsImmediately` — close the entrypoint; call `await(ctx, "msg")`; assert it returns within a few milliseconds and the pool entry is returned. Run — confirm passing.

### Phase 6: Add `admit()` gate (TDD)

- [x] Add `TestAdmit_TrueWhenBelowThreshold` — fresh entrypoint, empty work channel; assert `admit()` is true. Run — confirm failure (no `admit` yet → compile error).
- [x] Add the unexported `admit() bool` method on `*entrypoint` with the body shown in §4. Run — confirm `TrueWhenBelowThreshold` passes.
- [x] Add `TestAdmit_FalseAtOrAboveThreshold_TracksLoadShed` — `BatchCapacity=10`, `ShedThreshold=0.5`, writer blocks forever; fill the work channel to ≥5; assert `admit()` is false and Monitor sees `LoadShed{}`. Run — confirm passing.
- [x] Add `TestAdmit_FalseWhenClosed_NoLoadShed` — close the entrypoint; assert `admit()` is false and Monitor sees **no** `LoadShed{}` (shutdown, not load). Run — confirm passing.
- [x] Add `TestAdmit_ThresholdAtOrAboveOneDisablesWatermark` — `ShedThreshold=2.0`; fill the channel; assert `admit()` stays true until the pipeline is closed. Run — confirm passing.

### Phase 7: Add `AsHTTPHandler` decorator and `Admission` middleware (TDD, in-module)

- [x] Add `TestAsHTTPHandler_ForwardsSingleMessageToAwait` (in `admission_test.go`) — a fake that implements `messaging.Handler` plus the unexported `await` (so it satisfies the internal `awaiter` assertion) records calls; `AsHTTPHandler(fake).Handle(ctx, "x")`; assert exactly one `await(ctx, "x")`. Run — confirm failure (no `AsHTTPHandler` yet).
- [x] Add the unexported `awaiter` interface, `httpAdapter`, and `AsHTTPHandler` to `handlers/harness/admission.go`. Run — confirm passing.
- [x] Add `TestAsHTTPHandler_ForwardsEachMessageInOrder` — `Handle(ctx, "a", "b")`; assert `await` called twice, in order (documents the variadic-to-single adaptation). Run — confirm passing.
- [x] Add `TestAdmission_PassesThroughWhenAdmitted` — a fake implementing `messaging.Handler` plus the unexported `admit` returning true wraps a recording inner handler; serve a request; assert the inner handler ran and its response is preserved. Run — confirm failure (no `Admission` yet).
- [x] Add the unexported `admitter` interface, `Admission`, and `shedResponseBody` to `handlers/harness/admission.go`. Run — confirm `PassesThroughWhenAdmitted` passes.
- [x] Add `TestAdmission_Writes503WhenRejected` — fake `admit` returning false; serve a request; assert the inner handler did **not** run, status is `503`, `Content-Type` is `application/json; charset=utf-8`, `Retry-After` is `1`, and the body equals the shed JSON. Run — confirm passing.

### Phase 8: Pin the existing `Handle` contract (TDD, MQ/cron path)

- [x] Add `TestHandle_BlocksUntilDurable` — submit a batch via `Handle`; the writer takes a controlled delay to acknowledge; assert `Handle` returns only after the writer completes. Run — confirm passing (pins the contract `streaming` depends on).
- [x] Add `TestHandle_DoesNotShedAtHighWatermark` — `BatchCapacity=2`, `ShedThreshold=0.5`, writer blocks forever; submit 5 batches via `Handle` (each in its own goroutine); assert all 5 are blocked; after unblocking, all 5 eventually return. Run — confirm passing.
- [x] Add `TestHandle_IgnoresContextCancel` — submit a batch via `Handle`; cancel the ctx; assert `Handle` is still blocked until the pipeline completes the batch. Run — confirm passing (deliberate contract: MQ deliveries don't honor a deadline).
- [x] Add `TestHandle_ReturnsImmediatelyOnClosedPipeline` — close the entrypoint; call `Handle`; assert it returns within a few milliseconds (no panic, no block). Run — confirm passing.
- [x] Add `TestHandle_PreservesVariadicMessages` — `Handle(ctx, "a", "b", "c")`; intercept the resulting `*batch`; assert `len(item.messages) == 3`. Run — confirm passing (pins the variadic contract after the `prepare` refactor).

### Phase 9: Race and integration sanity

- [x] Run the full harness test suite under `-race`: `go test -race ./handlers/harness/...`. Confirm green. (Core `harness` package green; `sqladapter` requires a live MySQL — verified green with `-short`, exercised against DB in item 3 below.)
- [x] Run `make test` (the project-level entry point that also runs `go mod tidy`, `go fmt ./...`). Confirm green. (`go fmt` clean; full `-short -race` suite green. `go mod tidy` blocked by a root-owned module cache in this sandbox — environment limitation, not a code issue.)
- [x] Run `go test ./handlers/harness/sqladapter/...` against a live MySQL (drop `-short` if present, or use the project's `make test.db.local`-equivalent). Confirm no regressions in the SQL adapter. (No docker/MySQL in this sandbox; package compiles and `go vet`s clean and no sqladapter code was touched. **Follow-up: run `make test.db.local` locally to fully confirm.**)

### Phase 10: Documentation

- [x] Update `doc/work-sessions/2026/2026-05-14_pipeline-component-diagram.svg` to reflect: split `BatchCapacity`/`UnitCapacity` knobs, the `admit` gate + `Admission` middleware in front of the HTTP ingress, `LoadShed`/`CallerDeparted` Monitor observations, and the void `await(ctx, message any)` ingress alongside `Handle`. (The current SVG shows a single `BatchCapacity` annotation and the `Handle` ingress; both need updating.)
- [x] Self-review the diff: confirm no `messaging.Handler` callers were inadvertently broken; confirm `admission.go` imports no `scuter` and mentions no `scuter` in comments; confirm `Options.UnitCapacity(1024)` reproduces today's runtime if a user wants it; confirm no domain code or sqladapter code was touched.

### Out of scope (handled in each consuming repo's follow-on)

- Per-service route wireup (`AsHTTPHandler` + `Admission` in each routes table), application-side monitor metric registration, integration tests, and post-deploy observation drills. These are addressed per service after this module's changes merge and a version is tagged.
