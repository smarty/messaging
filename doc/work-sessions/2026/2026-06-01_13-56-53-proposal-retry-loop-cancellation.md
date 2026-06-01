---
name: Bound the unbounded retry loops in persistence and broadcast
description: Make the persistence and broadcast stages' retry loops cancellable via the pipeline context so a permanently-failing Writer/Dispatcher can no longer hang pipeline shutdown, while preserving store-and-forward durability (persistence is the ack gate; broadcast is recoverable via the startup recovery routine).
type: plot
---

# Proposal: Bound the unbounded retry loops in `persistence` and `broadcast`

## Background

Two pipeline stages retry their collaborator forever with no escape hatch:

`handlers/harness/03_persistence.go:39`

```go
for attempt := 1; ; attempt++ {
	err := this.writer.Write(this.ctx, this.buffer...)
	if err == nil {
		failure.Attempt = 0
		failure.Error = nil
		break
	}
	failure.Attempt = attempt
	failure.Error = fmt.Errorf("%w: %w", ErrPersistence, err)
	this.monitor.Track(failure)
	this.sleep(time.Second) // TODO: exponential back-off w/ jitter
}
```

`handlers/harness/05_broadcast.go:39` is identical in shape (against `Dispatcher.Dispatch`).

Both loops:

1. Have **no maximum attempt count** — they spin until the collaborator
   succeeds.
2. **Never consult `this.ctx.Done()`.** The pipeline context is passed *into*
   `Write`/`Dispatch` (so a ctx-honoring driver aborts the in-flight call), but
   the loop itself ignores cancellation and immediately retries.
3. Sleep via an **uninterruptible** `this.sleep(time.Second)` (wired to
   `time.Sleep` in `pipeline.go`).

### The failure mode

Shutdown drains the pipeline by closing channels from the front: `entrypoint.Close()`
closes the batches channel, `execution` then closes its output, and the close
cascades stage-by-stage (`for unit := range this.input` exits, then
`defer close(this.output)` fires). Each stage finishes the **unit it is
currently holding** before observing the closed input.

If the database (persistence) or broker (broadcast) is unreachable when shutdown
begins, the stage holding an in-flight unit never breaks out of its retry loop,
never returns to `range this.input`, never observes the closed channel, and
**never closes its output**. The drain stalls, the remaining stages never see
their inputs close, and shutdown hangs until the orchestrator SIGKILLs the
process after its grace period.

### The design constraint that shapes the fix

The pipeline is store-and-forward, and the stage order matters
(`handlers/harness/pipeline.go`):

```
entrypoint → execution → serialization → persistence → completion → broadcast → terminal
                                              (03)         (04)        (05)
```

`complete()` — which calls `waiter.Done()` (unblocking the `Handle`/`await`
caller) and lets the upstream MQ delivery be acked — fires in the **completion**
stage (04), which runs **after persistence (03)** and **before broadcast (05)**.

That ordering creates a hard asymmetry between the two failing stages:

| Stage              | Runs vs. ack   | On give-up, is the message recoverable?                                                                                          |
|--------------------|----------------|----------------------------------------------------------------------------------------------------------------------------------|
| `persistence` (03) | **before** ack | No. If the row was never written, forwarding it acks/loses the message. Only MQ redelivery recovers it.                          |
| `broadcast` (05)   | **after** ack  | Yes. The row is already durably stored; `sqladapter.Recover` re-dispatches every row `WHERE dispatched IS NULL` at next startup. |

The resilience proposal
(`2026-05-28_15-30-32-proposal-harness-resilience-module-changes.md`) already
ruled out per-batch context in these stages and fixed their cancellation scope:

> **Inject a per-batch `ctx` through Persistence and Broadcast.** Rejected.
> Per-batch ctx in retry-forever stages would unwind partially-completed work and
> break the durability principle. The pipeline ctx (`harness.New(ctx, …)`) is the
> right scope for those stages.

So the intended (but not-yet-implemented) cancellation signal for these loops is
the **pipeline context** — the `ctx` the consumer passes to `harness.New(ctx, …)`
and is expected to cancel on shutdown. This proposal wires that signal in.

## Approach

Make both retry loops **cancellable via `this.ctx`**, and act on cancellation
according to each stage's recoverability (the asymmetry above):

- **`broadcast` (recoverable):** on `this.ctx` cancellation, stop retrying and
  **forward the unit** to `terminal` as usual. The message is already persisted;
  `sqladapter.Recover` redispatches it on the next startup. No data loss, clean
  shutdown.

- **`persistence` (durability gate):** on `this.ctx` cancellation, stop retrying
  and **drop the unit without forwarding it** — `complete()` is never called, so
  the upstream MQ delivery is never acked and is redelivered on the next run. The
  stage then continues its `range` loop and drains normally, so shutdown
  proceeds. Forwarding here is *not* an option: it would reach the completion
  stage, ack the caller, and lose a message that was never stored.

#### HTTP-origin units (the `await` path)

The drop decision is identical for HTTP-submitted units, but the *recovery*
mechanism differs, because there is no broker to redeliver. When persistence
drops an HTTP-origin unit on shutdown:

- `complete()` never fires, so the in-flight `await` caller is **not** unblocked
  via the completion path. It unblocks instead when its own request context is
  cancelled (the `case <-ctx.Done()` arm of `await`'s second select), tracking
  `CallerDeparted` — exactly today's departed-caller behavior. Whether that
  request context is actually cancelled at shutdown is a consumer-side graceful-
  shutdown concern (does the HTTP server cancel active request contexts?), out of
  scope here.
- Recovery relies on **client retry + domain idempotency**, the model the
  resilience proposal already established for the HTTP path: a client whose
  request did not durably succeed retries, and once the merged domain-layer
  idempotency change is in place repeated retries collapse to no-ops after the
  first applies. There is no `sqladapter.Recover` safety net for an un-stored
  unit (recovery only re-dispatches rows that *were* stored), so the client retry
  is the sole recovery path — which is why dropping (rather than forwarding) is
  still the correct choice: forwarding would unblock the caller as though the
  write succeeded, falsely confirming a message that was never stored.

There is no regression versus today: an HTTP request in flight against a dead
database currently hangs until SIGKILL just the same; this change makes the
process shut down cleanly instead.

A new interruptible backoff replaces the uninterruptible sleep, so shutdown
aborts the delay promptly instead of waiting out a full second per attempt.

### The cancellable-wait seam

Replace the injected `sleep func(time.Duration)` with an injected, ctx-aware
waiter:

```go
// retry.go (new)
package harness

import (
	"context"
	"time"
)

// wait sleeps for d, or returns early with ctx.Err() if ctx is cancelled first.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

`pipeline.go` passes `wait` where it currently passes `time.Sleep`. Tests inject
a fixture method that records the requested durations (preserving the existing
`sleeps`-style assertions) and can simulate cancellation by returning a non-nil
error.

> **Why inject the wait function rather than a duration?** A reasonable
> alternative is to keep `wait` as a plain free function, store only a
> `time.Duration` on each stage, and have the stage call `wait(this.ctx, this.delay)`
> directly. That is cleaner production code. It is rejected for one reason: the
> **test seam**. The stages today inject `sleep func(time.Duration)` precisely so
> the fixtures can (a) record how many times and for how long the loop backed off
> and (b) run instantly without real 1-second sleeps. If we called the real `wait`
> directly, every retry test would block on actual wall-clock time and could not
> observe the backoff. Keeping the injected-function seam preserves the existing
> `TestRetriesUntil…Succeeds` assertions verbatim and lets a fixture simulate
> cancellation synchronously by returning an error on a chosen attempt. The free
> `wait` function still exists — it is simply the production value wired in at
> `pipeline.go`, while tests substitute their own. (When exponential backoff lands
> later, the *duration* moves inside `wait`/its production implementation; the
> injected-function seam stays, so this decision does not block that work.)

This keeps the fixed 1-second delay; **exponential back-off with jitter remains a
separate, orthogonal TODO** and is out of scope here — this change is strictly
about bounding/cancelling the loops.

### Revised `persistence`

```go
func (this *persistence) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		stored := this.store()
		this.buffer = this.buffer[:0]
		if !stored {
			continue // shutdown before durable write: do NOT forward (no ack); MQ redelivers
		}
		this.output <- unit
	}
}

func (this *persistence) store() (stored bool) {
	var failure PersistenceError
	for attempt := 1; ; attempt++ {
		err := this.writer.Write(this.ctx, this.buffer...)
		if err == nil {
			return true
		}
		failure.Attempt = attempt
		failure.Error = fmt.Errorf("%w: %w", ErrPersistence, err)
		this.monitor.Track(failure)
		if this.wait(this.ctx, time.Second) != nil {
			this.monitor.Track(PersistenceAbandoned{Attempts: attempt})
			return false
		}
	}
}
```

### Revised `broadcast`

```go
func (this *broadcast) Listen() {
	defer close(this.output)
	for unit := range this.input {
		for _, message := range unit.results {
			this.buffer = append(this.buffer, message)
		}
		this.dispatch()
		this.buffer = this.buffer[:0]
		this.output <- unit // always forward: already persisted; recovery redispatches if not sent
	}
}

func (this *broadcast) dispatch() {
	var failure BroadcastError
	for attempt := 1; ; attempt++ {
		err := this.dispatcher.Dispatch(this.ctx, this.buffer...)
		if err == nil {
			return
		}
		failure.Attempt = attempt
		failure.Error = fmt.Errorf("%w: %w", ErrBroadcast, err)
		this.monitor.Track(failure)
		if this.wait(this.ctx, time.Second) != nil {
			this.monitor.Track(BroadcastAbandoned{Attempts: attempt})
			return
		}
	}
}
```

### New Monitor observations (`contracts.go`)

```go
PersistenceAbandoned struct{ Attempts int } // emitted when persistence stops retrying due to shutdown; the unit was dropped and the upstream message will be redelivered
BroadcastAbandoned   struct{ Attempts int } // emitted when broadcast stops retrying due to shutdown; the message is persisted and will be redispatched by recovery
```

These give operators an explicit, alertable signal that a shutdown abandoned
in-flight work — distinct from the per-attempt `PersistenceError`/`BroadcastError`.

### Files created / modified

| File                                      | Change                                                                                      |
|-------------------------------------------|---------------------------------------------------------------------------------------------|
| `handlers/harness/retry.go`               | **New.** The `wait(ctx, d)` interruptible-backoff helper.                                   |
| `handlers/harness/03_persistence.go`      | Replace `sleep` field/param with `wait`; extract `store()`; drop-without-forward on cancel. |
| `handlers/harness/05_broadcast.go`        | Replace `sleep` field/param with `wait`; extract `dispatch()`; forward-on-cancel.           |
| `handlers/harness/contracts.go`           | Add `PersistenceAbandoned` and `BroadcastAbandoned` observation types.                      |
| `handlers/harness/pipeline.go`            | Pass `wait` to `newPersistence`/`newBroadcast` instead of `time.Sleep`.                     |
| `handlers/harness/03_persistence_test.go` | Rename `sleep`→`wait` seam; add ctx-cancel abandonment test (unit **not** forwarded).       |
| `handlers/harness/05_broadcast_test.go`   | Rename `sleep`→`wait` seam; add ctx-cancel abandonment test (unit **is** forwarded).        |

### Alternatives considered

- **Persistence keeps retrying forever (only fix broadcast).** Treat persistence's
  infinite retry as intentional durability behavior and rely on the orchestrator's
  grace-period + SIGKILL + MQ redelivery to bound shutdown. Simpler, and arguably
  "more durable" (never gives up). Rejected as the recommendation because it leaves
  the original concern — a hung shutdown when the DB is down — unresolved, trading a
  clean shutdown for a SIGKILL every time. Presented here as the headline open
  question (see below); it is a one-line variant of this proposal (skip the
  persistence drop path, keep the loop uninterruptible).

- **Bounded max-attempts, then forward.** Give each loop a retry ceiling and forward
  downstream when exhausted. Rejected: for persistence, forwarding an un-stored unit
  acks and loses the message; for broadcast it is unnecessary (recovery already
  handles it). A ceiling also turns transient-but-long outages into data loss during
  *normal* operation, not just shutdown.

- **Per-batch context.** Already rejected by the resilience proposal (would unwind
  partially-completed work); not revisited.

## Trade-offs & Risks

- **Decision (settled): persistence drops the in-flight unit on shutdown.** On
  shutdown with a dead database, `persistence` stops retrying and drops the
  in-flight unit (clean shutdown; the un-acked MQ delivery is redelivered, or the
  HTTP client retries). The rejected alternative — keep retrying forever, bounded
  only by orchestrator SIGKILL — is retained in *Alternatives considered* for the
  record. Broadcast is unaffected by this choice (forward-on-cancel either way).

- **Dropped-unit caller blocks until process exit.** When persistence
  drops a unit on shutdown, its `complete()` never fires, so an in-flight
  `Handle`/`await` caller stays blocked on `waiter.Wait()`. This is acceptable during
  shutdown — the consumer cancels `this.ctx`, the MQ delivery framework tears those
  goroutines down, and the un-acked message is redelivered. It does mean a
  `goleak`-style "no leaked goroutines" assertion must not be applied across the
  shutdown-abandonment path.

- **Cancellation depends on collaborators honoring `ctx`.** If `Writer.Write` /
  `Dispatcher.Dispatch` block forever ignoring the passed `ctx`, our loop cannot
  reach the `wait` check until that call returns. The `sqladapter` Writer/Dispatcher
  use `BeginTx(ctx)`/`ExecContext(ctx)`/ctx-scoped connector calls, so they abort
  promptly on cancellation. Worth stating as a documented contract for custom
  collaborators.

- **Requires the consumer to cancel the pipeline ctx on shutdown.** The retry loops
  abort on `this.ctx` cancellation; the channel-close cascade alone does not cancel
  `this.ctx`. If a consumer wires `harness.New` with a `context.Background()` that is
  never cancelled, the in-flight retry remains unbounded (it still drains correctly
  on the *next* unit boundary, but the currently-held unit can still hang). This is
  the intended contract per the resilience proposal; it should be called out in the
  package doc.

- **Deterministic tests are straightforward here** (unlike the waiter-pool fix): the
  injected `wait` seam returns a chosen error synchronously, so abandonment is
  exercised without real timing or concurrency races.

- **No change to the happy path or to the fixed-1s backoff.** Existing retry tests
  keep asserting two 1-second waits before success; only the seam's name/signature
  changes.

## Implementation Checklist

### Phase 1: Cancellable-wait seam (refactor, stays green)

- [x] Add `handlers/harness/retry.go` with the `wait(ctx context.Context, d time.Duration) error` helper shown in Approach.
- [x] In `pipeline.go`, change `newPersistence(...)` and `newBroadcast(...)` call sites to pass `wait` instead of `time.Sleep`.
- [x] In `03_persistence.go` and `05_broadcast.go`, change the struct field and constructor parameter from `sleep func(time.Duration)` to `wait func(context.Context, time.Duration) error` (no behavior change yet — call `this.wait(this.ctx, time.Second)` and ignore the returned error so the loops still spin forever).
- [x] In both `*_test.go` fixtures, rename the `sleep`/`sleeps` seam to a `wait`/`waits` method matching the new signature, recording durations and returning `nil`.
- [x] Run `make test` — confirm green (pure refactor; existing `TestRetriesUntil…Succeeds` still sees two recorded 1s waits).

### Phase 2: Broadcast cancellation — forward on shutdown (red → green)

- [x] Add `BroadcastAbandoned struct{ Attempts int }` to `contracts.go`.
- [x] Add `TestBroadcastAbandonsOnContextCancelButStillForwards`: dispatcher always fails; fixture `wait` returns `context.Canceled` on its first call. Expect failure (compile error — `BroadcastAbandoned` referenced before the loop emits it, or assertion fails because the current loop ignores the wait error and spins). Record the failure reason.
- [x] Run the new test, confirm it fails for the right reason. (Observed: 5s `-timeout` kill — the loop ignored the wait error and spun forever against the always-failing dispatcher, never forwarding or tracking abandonment.)
- [x] Implement: extract `dispatch()`; on `this.wait(...) != nil`, `Track(BroadcastAbandoned{Attempts: attempt})` and return; `Listen()` still forwards the unit to `output` afterward.
- [x] Run the test — confirm: exactly one `Dispatch` call, one recorded wait, one `BroadcastError` then one `BroadcastAbandoned` tracked, and the unit **was** forwarded to `output`.
- [x] Confirm `TestRetriesUntilDispatchSucceeds` and the other broadcast tests still pass.

### Phase 3: Persistence cancellation — drop without forwarding (red → green)

- [x] Add `PersistenceAbandoned struct{ Attempts int }` to `contracts.go`.
- [x] Add `TestPersistenceAbandonsOnContextCancelAndDropsUnit`: writer always fails; fixture `wait` returns `context.Canceled` on its first call. Expect failure (the current loop ignores the wait error and spins, so the test would hang / never see abandonment). Record the failure reason.
- [x] Run the new test, confirm it fails for the right reason. (Observed: 5s `-timeout` kill — the loop ignored the wait error and spun forever against the always-failing writer, never dropping the unit or tracking abandonment.)
- [x] Implement: extract `store() bool`; on `this.wait(...) != nil`, `Track(PersistenceAbandoned{Attempts: attempt})` and return `false`; `Listen()` skips `output <- unit` (does **not** forward) when `store()` returns false, resets the buffer, and continues the range loop.
- [x] Run the test — confirm: exactly one `Write` call, one recorded wait, one `PersistenceError` then one `PersistenceAbandoned` tracked, and the unit was **not** forwarded to `output` (output closes empty after input closes).
- [x] Confirm `TestRetriesUntilWriteSucceeds` and the other persistence tests still pass.

### Phase 4: Full verification & conventions

- [x] Run `make test` (fmt, vet, `-race`, coverage) — confirm green and that `handlers/harness` coverage has not regressed. (All packages pass; `handlers/harness` coverage 97.7%, unchanged.)
- [x] Add a short note to the `package harness` doc comment in `config.go`: the persistence/broadcast retry loops abort on cancellation of the context passed to `New(ctx, …)`; consumers must cancel it on shutdown, and custom `Writer`/`Dispatcher` implementations must honor the context they are given.
- [x] Re-read the diff against the `CLAUDE.md` Go conventions: receiver named `this`; named slice/return values where applicable; no naked returns; no blank lines at method start/end; struct initializers use field/value pairs; multi-line struct literals close the brace on their own line.
