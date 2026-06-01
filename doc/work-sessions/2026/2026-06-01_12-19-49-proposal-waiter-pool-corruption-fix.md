---
name: Fix pooled WaitGroup corruption in entrypoint.await
description: Stop returning a pooled *sync.WaitGroup to the pool while its count is non-zero and a detached Wait() goroutine still references it, eliminating a sync.WaitGroup-misuse / cross-request corruption hazard on the caller-departed path.
type: plot
---

# Proposal: Fix pooled `WaitGroup` corruption in `entrypoint.await`

## Background

`handlers/harness/00_entrypoint.go` exposes two ingress paths into the pipeline:

- `Handle(ctx, messages...)` — the at-least-once (RMQ) path. It enqueues a batch
  and blocks **inline** on `waiter.Wait()` until the pipeline calls `complete()`.
- `await(ctx, message)` — the context-honoring (HTTP) path. It enqueues a batch
  and then waits on **either** completion **or** `ctx.Done()`, so a departed
  caller (cancelled request) does not block on durable processing.

Both paths share `prepare(...)`, which leases a `*sync.WaitGroup` from a
`sync.Pool` (`this.waiters`), calls `waiter.Add(1)`, and captures that waiter in
the batch's `complete` closure:

```go
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
```

`complete()` is invoked **later**, by the pipeline's `completion` stage
(`04_completion.go`), once the batch is durably persisted.

### The bug

`await` returns the waiter to the pool unconditionally via `defer`:

```go
func (this *entrypoint) await(ctx context.Context, message any) {
	...
	waiter, item := this.prepare(ctx, message)
	defer this.waiters.Put(waiter)            // (B) blanket Put — the defect

	select {
	case this.work <- item:
		this.lock.RUnlock()
		this.monitor.Track(batchInFlight)
	case <-ctx.Done():
		this.lock.RUnlock()
		this.abandon(waiter, item)            // calls waiter.Done() -> count 0
		this.monitor.Track(callerDeparted)
		return
	}

	select {
	case <-this.waiterDone(waiter):           // detached goroutine: waiter.Wait(); close(done)
	case <-ctx.Done():                        // (A) caller departed WHILE waiting
		this.monitor.Track(callerDeparted)
	}
}
```

On path **(A)** — the caller's context is cancelled *after* the batch was
successfully enqueued — `await` returns and the deferred `Put` (B) returns the
waiter to the pool. But at that instant:

1. The pipeline still owns the batch and **has not yet called `complete()`**, so
   the waiter's internal counter is still `1` (the `Add(1)` from `prepare` has no
   matching `Done()` yet).
2. The detached goroutine spawned by `waiterDone(waiter)` is **still blocked
   inside `waiter.Wait()`**, holding a live reference to the same waiter.

A subsequent `prepare()` can then `Get()` that very waiter and call `Add(1)`
again. Per the `sync.WaitGroup` contract, *"if a WaitGroup is reused to wait for
several independent sets of events, new `Add` calls must happen after all
previous `Wait` calls have returned"*. Here a new `Add` races a previous `Wait`
that has not returned, which is documented misuse. Consequences range from the
Go runtime panicking (`sync: WaitGroup is reused before previous Wait has
returned` / `WaitGroup misuse: Add called concurrently with Wait`) to silent
cross-request corruption, where one HTTP request's `await` blocks until an
unrelated earlier batch also completes.

### Why the other paths are safe (and stay safe)

| Path                               | State of waiter when returned to pool            | Detached `Wait()` goroutine?    | Safe?  |
|------------------------------------|--------------------------------------------------|---------------------------------|--------|
| `Handle` normal                    | `Wait()` returned inline → count 0               | none (waits inline)             | yes    |
| `await` enqueue-failed (`abandon`) | `abandon` called `Done()` → count 0              | none (2nd select not reached)   | yes    |
| `await` normal completion          | `waiterDone` fired → `Wait()` returned → count 0 | finished before `done` received | yes    |
| `await` departed-while-waiting (A) | count still 1, `complete()` pending              | **still blocked in `Wait()`**   | **NO** |

Only path (A) is defective. The fix must keep the safe paths recycling waiters
(the pool exists to cut allocations under load) while never recycling a waiter
that is still "in use."

## Approach

**Recommended: Option A — targeted lifecycle fix (minimal diff).**

Remove the blanket `defer this.waiters.Put(waiter)` and instead return the
waiter to the pool *only* at the three points where it is provably quiescent
(counter at 0, no detached goroutine still reading it). On the departed-while-
waiting path, deliberately do **not** recycle the waiter: let it fall out of
scope. `complete()` will still fire later, unblocking the detached `waiterDone`
goroutine, after which both the waiter and the goroutine become garbage. We
"lose" exactly one pooled waiter per departed-in-flight request — an exceptional
path — which `sync.Pool` is designed to tolerate.

Revised `await`:

```go
func (this *entrypoint) await(ctx context.Context, message any) {
	this.lock.RLock()
	if this.closed {
		this.lock.RUnlock()
		return
	}

	waiter, item := this.prepare(ctx, message)

	select {
	case this.work <- item:
		this.lock.RUnlock()
		this.monitor.Track(batchInFlight)
	case <-ctx.Done():
		this.lock.RUnlock()
		this.abandon(waiter, item)
		this.waiters.Put(waiter) // safe: abandon() called Done() (count 0); no detached waiter.
		this.monitor.Track(callerDeparted)
		return
	}

	select {
	case <-this.waiterDone(waiter):
		this.waiters.Put(waiter) // safe: detached Wait() returned before done was closed (count 0).
	case <-ctx.Done():
		this.monitor.Track(callerDeparted)
		// Intentionally do NOT recycle the waiter here: complete() is still
		// pending and the detached waiterDone goroutine is still inside Wait().
		// Returning it to the pool now would let a later prepare() call Add(1)
		// before this Wait() returns -- documented sync.WaitGroup misuse. The
		// waiter is released to GC once complete() fires.
	}
}
```

`Handle` is unchanged: its `defer this.waiters.Put(waiter)` runs only after its
inline `waiter.Wait()` returns, so the waiter is quiescent at recycle time.

`prepare`, `abandon`, and `waiterDone` are unchanged.

### Alternative considered — Option B: replace the pooled WaitGroup with a single-use completion channel

Each ingress waits for **exactly one** `complete()` call, so a `sync.WaitGroup`
is heavier than needed. We could give `batch` a `done chan struct{}` that
`complete()` closes once; `Handle` does `<-done` and `await` does
`select { case <-done: ... case <-ctx.Done(): ... }`. This eliminates the
detached `waiterDone` goroutine and the entire pool-reuse hazard class, because
a channel is single-use and never recycled — an abandoned `done` channel is
simply collected by GC.

Rejected as the primary fix because:

- It is a larger change that touches `Handle`'s hot path and removes the
  `waiters` pool the author added deliberately for allocation reduction.
- It trades pooled-waiter reuse for a per-request channel allocation.
- The user's request is scoped to *resolving the corruption concern*, and
  Option A does so with a minimal, reviewable diff while preserving the
  existing design intent and all current passing tests.

Option B remains a reasonable future simplification if the detached goroutine or
per-request waiter churn proves costly; noting it here so the decision is
explicit. **Open question for the reviewer:** prefer the minimal Option A, or
take the larger Option B cleanup now?

## Trade-offs & Risks

- **One pooled waiter is dropped per departed-in-flight request.** This is the
  exceptional path (HTTP caller cancelled after enqueue). Under sustained
  cancellation storms the pool simply allocates fresh waiters; `sync.Pool`
  absorbs this without leaking memory (dropped waiters are GC'd once their
  pending `complete()` fires). No unbounded growth.
- **The detached `waiterDone` goroutine still lingers until `complete()`** on the
  departed path. This is pre-existing behavior and is bounded by pipeline drain
  time; the work is genuinely still in flight, so tracking its completion with
  one goroutine is acceptable. Option A does not worsen this; Option B would
  remove it.
- **Deterministic red-test difficulty.** The corruption only manifests on pool
  *reuse* under concurrency, and `sync.Pool.Get` gives no reuse guarantee, so a
  strictly deterministic failing unit test is not practical. The honest red test
  is a race-enabled stress test (see Phase 1) that reliably trips Go's built-in
  `WaitGroup` misuse detector before the fix and passes after. This is called out
  explicitly rather than hidden behind a false "deterministic" claim.

## Implementation Checklist

### Phase 1: Capture the corruption (red)

- [x] Add `TestAwait_DepartedInFlightDoesNotCorruptPooledWaiter` to
  `handlers/harness/00_entrypoint_test.go`. The test, in a loop sized for
  reliable surfacing (e.g. a few thousand iterations), should: (1) start an
  `await` whose context it cancels *after* the batch is received from
  `this.work` (departed-while-waiting), deferring the matching `complete()` to a
  background goroutine; (2) concurrently issue fresh `await`/`Handle` calls on
  the **same** entrypoint that drive `prepare()` (and thus pool `Get`/`Add`),
  completing each; so a recycled-too-early waiter is exercised by a new request.
  (Implemented as 8 worker goroutines × 2000 departing `await`s against a
  concurrent drainer that completes each enqueued batch — this produces the
  recycle/reuse race more reliably than a sequential loop.)
- [x] Run `go test -race -run TestAwait_DepartedInFlightDoesNotCorruptPooledWaiter ./handlers/harness/`
  and confirm it fails for the right reason: a `sync: WaitGroup ...` misuse panic
  or a race report on the waiter (NOT a generic assertion mismatch). Record the
  observed failure mode in the PR description.
  **Observed:** `WARNING: DATA RACE` on the waiter (read in `await` at
  `00_entrypoint.go:80` vs. write from the detached `waiterDone` goroutine) and
  `panic: sync: WaitGroup is reused before previous Wait has returned`.

### Phase 2: Apply the lifecycle fix (green)

- [x] In `00_entrypoint.go`, remove `defer this.waiters.Put(waiter)` from
  `await`.
- [x] In `await`, add `this.waiters.Put(waiter)` immediately after
  `this.abandon(waiter, item)` on the enqueue-failed branch.
- [x] In `await`, add `this.waiters.Put(waiter)` in the
  `case <-this.waiterDone(waiter):` branch (normal completion).
- [x] In `await`, in the `case <-ctx.Done():` branch of the second `select`,
  add the explanatory comment documenting why the waiter is intentionally NOT
  recycled there.
- [x] Confirm `Handle` is unchanged and still recycles via its inline-`Wait()`
  `defer this.waiters.Put(waiter)`.

### Phase 3: Verify (green) and guard regressions

- [x] Run `go test -race -run TestAwait_DepartedInFlightDoesNotCorruptPooledWaiter ./handlers/harness/`
  and confirm it now passes with no panic and a clean race report.
- [x] Confirm the existing departed-path tests still pass unchanged:
  `TestAwait_UnblocksOnContextCancelWhileWaiting` (departed-while-waiting still
  completes and tracks `CallerDeparted` + later `BatchComplete`) and
  `TestAwait_UnblocksOnContextCancelWhileEnqueuing` (abandon path still tracks
  `CallerDeparted`, no `BatchInFlight`/`BatchComplete`).
- [x] Run the full package suite: `make test` (exercises `go fmt`, `go vet`,
  `-race`, coverage). Confirm green. (All packages pass; `handlers/harness`
  coverage 99.6%.)
- [x] Re-read the diff against `## CLAUDE.md` Go conventions: receiver named
  `this`, no naked returns, no new blank lines at method start/end, struct
  initializers use field/value pairs (no new initializers introduced here).
