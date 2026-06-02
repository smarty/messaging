---
name: Stop double-encoding payloads across dispatch and recovery
description: Eliminate the redundant second serialization in the sqladapter Dispatcher and propagate ContentType through the in-memory harness Message — closing the TODO at handlers/harness/sqladapter/dispatcher.go:58 and fixing the recovery-path content-type and Topic mishandling. No schema changes.
type: plot
---

# Proposal: Stop double-encoding payloads across dispatch and recovery

## Background

The harness pipeline is store-and-forward: each message is serialized once
(stage 02), durably stored (stage 03), then published to the broker (stage 05
via `Dispatcher.Dispatch`). On startup, `sqladapter.Recover` re-publishes any
row whose `dispatched` column is still `NULL`.

The bytes that get stored in the `Messages.payload` column **must** be byte-
identical to the bytes that get published to the broker — otherwise a recovery
re-publishes a different message than the original send, silently. Today the
code does not guarantee this, and in one configuration it cannot publish a
recovered row at all.

There are three connected defects across `handlers/harness/02_serialization.go`,
`handlers/harness/sqladapter/dispatcher.go`, and
`handlers/harness/sqladapter/recovery.go`. They are usually invisible (the
production serializer is JSON, which makes the bug benign) and they are masked
by the unit-test stub connector — but they are real, and recovery against a
real RabbitMQ writer is broken today.

### Defect 1: payload is encoded twice on the happy path

`02_serialization.go:29` encodes `message.Value` into `message.Content` (these
are the bytes destined for the durable row).

`sqladapter/dispatcher.go:64-67` then builds a `messaging.Dispatch` with
`Message: message.Value` (the in-memory Go struct) and passes it to the
transport `Writer`. The transport writer is the `serialization.defaultWriter`
(`serialization/connector.go:103`), which loops calls into
`defaultDispatchEncoder.Encode` (`serialization/dispatch_encoder.go:30`). That
encoder sees `dispatch.Payload` is empty and `dispatch.Message` is non-nil, and
**runs the same `Serializer.Serialize` again** to produce
`dispatch.Payload`/`dispatch.ContentType`/`dispatch.MessageType`.

So the value is serialized once for the DB, then again for the broker. Both
encodings happen with the same `Serializer`, so the resulting bytes *are* equal,
but the harness has no guarantee of that — anyone who plugs in a serializer
with side effects, IDs, or timestamps will get divergent stored vs. published
bytes. And it is wasted CPU on every send.

The TODO at `dispatcher.go:58-63` calls this out and asks for one of two fixes:

> Either pass the pre-encoded bytes through Dispatch.Payload/MessageType/ContentType
> and skip the connector's serialization for this writer, or drop the harness
> Serialization stage and let the connector own all encoding.

### Defect 2: `Message.ContentType` is never populated on the happy path

`harness.Message` already has `ContentType` and `ContentEncoding` fields
(`message.go:22-28`) — but `02_serialization.go` only writes
`message.ContentType` on the **fallback** path (when the user's serializer
returned an error and we fall back to `fmt.Sprintf("%#v")`):

```go
err := this.serializer.Serialize(message.Content, message.Value)
if err != nil {
    // …
    message.ContentType = "go fmt.Sprintf(%#v)"
    _, _ = fmt.Fprintf(message.Content, "%#v", message.Value)
}
```

On the success path `message.ContentType` is left as the zero value `""`. The
field exists, and the dispatcher never reads it.

This is fine *today* because `dispatcher.go` re-encodes via the connector
(Defect 1), so the connector reapplies its own `ContentType()`. The moment we
fix Defect 1 by passing pre-encoded bytes through, we have to know what the
content type is — and the harness never recorded it. The fix lives entirely
in process memory: stage 02 stamps `message.ContentType` from
`serializer.ContentType()` so the dispatcher (next stage in the same process)
can read it. **No schema change.**

### Defect 3: recovery hands the dispatcher a malformed message and breaks against a real broker

`recovery.go:42-47` builds a `harness.Message` from the row:

```go
messages = append(messages, &harness.Message{
    ID:          id,
    Type:        typeName,
    Content:     bytes.NewBuffer(payload),
    ContentType: "application/json",
})
```

`Value` is left nil. The dispatcher today (per Defect 1) passes
`Message: message.Value` to the connector — which means recovery passes
`Message: nil` and `Payload: nil`. The connector's encoder takes its
`dispatch.Message == nil` early-return (`dispatch_encoder.go:31`) and **never
sets `Topic` / `MessageType` / `ContentType`**. The RabbitMQ writer then
rejects with `ErrEmptyDispatchTopic` (`rabbitmq/writer.go:38`). So in the only
production configuration that uses `topicFromMessageType=true`, recovery can't
publish a single row. The current dispatcher unit tests don't catch this
because the stub connector (`dispatcher_test.go:111`) ignores the topic field
entirely.

Defect 3 is **fixed automatically** once Defect 1 is fixed: the new dispatcher
reads `Type` / `ContentType` / `Content` (which recovery already populates)
from the `*harness.Message` and hands them to the connector as pre-populated
`MessageType` / `ContentType` / `Payload` / `Topic`. The connector encoder's
`len(Payload) > 0` early-return then leaves everything alone, and the broker
gets a complete dispatch.

The hardcoded `"application/json"` in recovery is **kept** — we deliberately
do not modify the `Messages` schema (the table holds 50M+ rows; we don't
extend it for this), and the project's standing assumption is that the
durable bytes are JSON. We document that assumption explicitly rather than
carry it implicitly.

### Why fix Defects 1 and 2 together

Defect 2 is the prerequisite for fixing Defect 1: the dispatcher can only
hand pre-encoded `ContentType` to the connector if the serialization stage
recorded it. The two changes ride together, and they fix Defect 3 as a
side effect.

## Approach

We pick the "pass-through" arm of the TODO: the harness Serialization stage
remains the single source of truth for encoded bytes and content type; the
sqladapter Dispatcher hands those pre-encoded bytes to the connector via
`Dispatch.Payload` / `MessageType` / `ContentType`; the connector encoder
short-circuits because `Payload` is non-empty. Recovery does the same
pass-through — using the row's stored `type` and the project-wide assumption
that stored bytes are `application/json`.

This is the smaller, safer arm. The alternative (delete the harness
Serialization stage entirely and let the connector own all encoding) would
require persistence to also call the connector's encoder and would entangle
the harness with the connector. Rejected on those grounds.

### The three changes

**Change A (`02_serialization.go`):** record the content type on the success
path. We need `serializer.ContentType()` on the internal `serializer`
interface — which today is just `Serialize(io.Writer, any) error`
(`contracts.go:34`). Extend it:

```go
serializer interface {
    Serialize(out io.Writer, in any) error
    ContentType() string
}
```

Then in `serialization.Listen`:

```go
err := this.serializer.Serialize(message.Content, message.Value)
if err == nil {
    message.ContentType = this.serializer.ContentType()
} else {
    // existing fallback unchanged: ContentType already set to the sentinel
    // "go fmt.Sprintf(%#v)" by the fallback branch.
}
```

The `nop` serializer in `config.go:112` gains a `ContentType() string { return "" }`
implementation, the test fixture in `02_serialization_test.go:40` gains the
same, and the production wiring (where callers pass
`serialization.Serializer` from the `serialization` package — which already
exposes `ContentType() string`, `contracts.go:9-11`) needs no changes since
that interface already satisfies the extended one.

**Change B (`sqladapter/dispatcher.go`):** stop passing `Value`; pass the
pre-encoded fields:

```go
dispatches = append(dispatches, messaging.Dispatch{
    Durable:     true,
    MessageType: message.Type,
    ContentType: message.ContentType,
    Payload:     message.Content.Bytes(),
    Topic:       message.Type, // matches connector's topicFromMessageType=true behavior
})
```

Why `Topic: message.Type` here? The connector's encoder normally sets
`Topic = MessageType` *only when* its `topicFromMessageType` flag is on
(`dispatch_encoder.go:55`). With `Payload` already populated, the encoder takes
its `len(dispatch.Payload) > 0` early-return (`dispatch_encoder.go:31`) and
never runs the topic-population block. So the dispatcher must populate Topic
itself, and Topic equals Type for this pipeline. (If a future deployment needs
a different topic-derivation rule, that rule will have to be wired into the
sqladapter Dispatcher explicitly — there is no longer a shared encoder
deciding it.) That deviation from the connector's behavior is **intentional
and noted** as a trade-off below.

**Change C (`sqladapter/recovery.go`):** no functional change; tighten the
construction comment to acknowledge the JSON assumption.

The current code already populates everything Change B's dispatcher needs:

```go
messages = append(messages, &harness.Message{
    ID:          id,
    Type:        typeName,
    Content:     bytes.NewBuffer(payload),
    ContentType: "application/json",
})
```

`Type` and `Content` come from the row; `ContentType` is the project-wide
assumption that stored bytes are JSON. With Change B in place, the dispatcher
reads these and the broker dispatch is well-formed. The change is to:

- Replace the zero-value `// hardcoded` framing with a brief comment that
  states the assumption: the durable column holds JSON because the
  configured serializer is JSON; recovery cannot recover what was not
  recorded, so it carries that assumption forward.

That's it — no SQL or schema change.

### Files modified

| File                                                  | Change                                                                                                  |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `handlers/harness/contracts.go`                       | Extend internal `serializer` interface with `ContentType() string`.                                     |
| `handlers/harness/02_serialization.go`                | On success, set `message.ContentType = this.serializer.ContentType()`.                                  |
| `handlers/harness/02_serialization_test.go`           | Update fixture serializer to satisfy `ContentType() string`; assert ContentType propagates.             |
| `handlers/harness/config.go`                          | `nop.ContentType() string { return "" }`.                                                               |
| `handlers/harness/sqladapter/dispatcher.go`           | Pass `Payload`/`MessageType`/`ContentType`/`Topic` from `*harness.Message`; remove TODO.                |
| `handlers/harness/sqladapter/dispatcher_test.go`      | Assert published Dispatch carries Payload/MessageType/ContentType/Topic; tighten stub connector.        |
| `handlers/harness/sqladapter/recovery.go`             | Comment-only: state the JSON-content-type assumption inline.                                            |
| `handlers/harness/sqladapter/recovery_test.go`        | Add a regression test asserting the recovered Dispatch carries `Topic`/`MessageType`/`Payload`/`ContentType`. |

No schema files (`doc/mysql/schema.sql`, `testdb_test.go`) are touched.

### Alternatives considered

- **Drop the harness Serialization stage; let the connector serialize.** The
  other arm of the TODO. Rejected: the persistence stage would have to call
  the connector's encoder before INSERT (otherwise we lose the
  content-type-at-store-time invariant), which entangles the harness with the
  serialization-connector and changes the order of failure (a connector
  encoding bug would now poison persistence rather than just dispatch).
  Pass-through is the smaller change.

- **Add a `content_type` column to `Messages` so recovery is faithful to a
  serializer change.** Rejected: the table holds 50M+ rows, the project-
  standing assumption is JSON, and the operational cost of an `ALTER TABLE` on
  that table outweighs the value of recovering exotic content types we don't
  use. The assumption is documented explicitly in code instead.

- **Have the dispatcher consult the connector's `topicFromMessageType` setting
  to decide whether to populate Topic.** Rejected: the sqladapter dispatcher
  has no handle to that config, the configuration is owned by a different
  package, and the dispatcher's contract is "publish what was persisted." For
  this pipeline, Topic = Type is the rule; encoding it explicitly in the
  dispatcher makes the rule visible at the call site instead of hidden in
  another package's option flag.

## Trade-offs & Risks

- **Recovery carries a JSON assumption, not a recorded fact.** With no
  `content_type` column on `Messages`, recovery cannot know what serializer
  was in effect when a row was written; it assumes `application/json`. If
  the configured serializer is ever changed to something else, in-flight
  rows persisted under the old serializer will be re-dispatched on next
  startup tagged `application/json` regardless of what they actually contain.
  Mitigation: drain the queue (no `dispatched IS NULL` rows) before swapping
  serializers. Acceptable risk because the project has historically used JSON
  and has no current plan to change.

- **Topic = MessageType is now hardcoded in the sqladapter Dispatcher.** This
  matches the connector's default behavior for the existing deployments
  (`topicFromMessageType=true`) but it removes the option flag's reach for the
  pre-encoded path. If a future deployment needs `Topic != MessageType`, the
  rule has to be wired explicitly into the dispatcher (a function, a map, or
  a new field on `harness.Message`). Worth flagging in a code comment so a
  future reader understands why the topic rule was duplicated here.

- **The connector encoder's `len(Payload) > 0` early-return is now load-bearing
  for correctness, not just optimization.** The contract becomes: "if Payload
  is set, leave the dispatch alone; the caller has populated everything that
  the broker needs." That's already its behavior
  (`dispatch_encoder.go:31`), and a short test in
  `serialization/dispatch_encoder_test.go` already asserts this (`TestWhen-
  DispatchAlreadyContainsSerializedPayload_Nop`); we'll cross-reference it in
  a comment but not duplicate it.

- **The dispatcher unit test that previously asserted `published[0].Message`
  must change** — the stub connector observes `Message` today, but after the
  change the dispatcher passes `Payload` and leaves `Message` nil. The test
  becomes: assert `Payload` is the persisted bytes, `MessageType` is the
  registered name, `ContentType` is what the harness recorded, `Topic` equals
  MessageType, `Durable` is true.

- **Coverage and shape of behavior tests are otherwise unchanged.** No new
  goroutine semantics, no concurrency-shape changes; this is a pure data-flow
  refactor — strictly in-process, no schema migration.

## Implementation Checklist

### Phase 1: ContentType on the internal serializer interface (red → green)

- [ ] Extend `02_serialization_test.go` fixture: add `ContentType() string { return "test/content-type" }` and a new test `TestSerializesEachResultValueIntoContent_PopulatesContentTypeOnSuccess` asserting `units[0].results[0].ContentType == "test/content-type"`.
- [ ] Run tests, confirm failure (assertion fails: success path leaves `ContentType` empty).
- [ ] Extend `serializer` interface in `handlers/harness/contracts.go` to include `ContentType() string`.
- [ ] Add `ContentType() string { return "" }` to `nop` in `config.go` so the default still satisfies the interface.
- [ ] In `02_serialization.go`, on the success branch, set `message.ContentType = this.serializer.ContentType()`. Leave the fallback branch's existing `"go fmt.Sprintf(%#v)"` assignment alone.
- [ ] Run tests, confirm green. Verify the fallback test (`TestSerializerErrorIsTracked_FallbackToFmtSprintfEncoding`) still asserts the `"go fmt.Sprintf(%#v)"` ContentType.

### Phase 2: Dispatcher pass-through (red → green)

- [ ] Add `TestDispatch_PublishesPreEncodedPayloadAndMetadata` to `dispatcher_test.go`: extend the `seedMessage` helper (or inline its setup) so the `*harness.Message` carries `Type`, `ContentType` (e.g. `"application/json"`), and a pre-encoded `Content` buffer. Assert the recorded `messaging.Dispatch` has `Payload == message.Content.Bytes()`, `MessageType == message.Type`, `ContentType == message.ContentType`, `Topic == message.Type`, `Durable == true`, and `Message == nil`.
- [ ] Run tests, confirm failure (today the dispatcher sends `Message: message.Value` and leaves Payload/MessageType/ContentType/Topic blank).
- [ ] Edit `sqladapter/dispatcher.go:55-71`:
    - Drop the TODO comment block.
    - Change the `messaging.Dispatch` literal to `{Durable: true, MessageType: message.Type, ContentType: message.ContentType, Payload: message.Content.Bytes(), Topic: message.Type}`.
    - Add a one-line comment explaining that Topic = Type because the connector encoder is short-circuited by the pre-populated Payload (cross-reference `serialization/dispatch_encoder_test.go:TestWhenDispatchAlreadyContainsSerializedPayload_Nop`).
- [ ] Run tests, confirm green. Update `TestDispatch_PublishesAndMarksDispatched` so its `published[0].Message` assertion is replaced with assertions on Payload/MessageType/ContentType/Topic.
- [ ] Confirm `TestDispatch_PublishFails_ReturnsErrorWithoutMarkingDispatched` and `TestDispatch_NoMessages_NoOp` still pass.

### Phase 3: Recovery regression test + comment (red → green)

- [ ] Add `TestRecover_PublishesDispatchWithTopicMessageTypeAndPayload`: seed an undispatched row with `type='order-received'` and a JSON payload; run `Recover`; assert the resulting `messaging.Dispatch` has `Topic == "order-received"`, `MessageType == "order-received"`, `ContentType == "application/json"`, `Payload == <the row's payload bytes>`, `Durable == true`, and `Message == nil`.
- [ ] Run the test. With Phase 2 in place, this test should already pass (recovery's `*harness.Message` is well-formed and the new dispatcher reads it correctly). If it fails, that is the right reason to fix it before continuing.
- [ ] Edit `sqladapter/recovery.go:42-47`: replace the inline framing of `ContentType: "application/json"` with a comment that states the assumption — the project's serializer is JSON; the durable bytes are recorded without a content-type column, so recovery carries that assumption forward.
- [ ] Run tests, confirm green. Confirm `TestRecover_NoOrphans_NoOp`, `TestRecover_DispatchesUndispatchedRowsInIDOrder`, `TestRecover_PassesPayloadAndTypeIntoMessage`, `TestRecover_RowsExceedBatchSize_FlushesInBatchesAndDispatchesAll`, and `TestRecover_RowCountIsMultipleOfBatchSize_FlushesUniformBatches` all still pass.

### Phase 4: Full verification

- [ ] Run `make test` (fmt, vet, `-race`, coverage). Confirm green and that `handlers/harness` and `handlers/harness/sqladapter` coverage have not regressed.
- [ ] Re-read the diff against the `CLAUDE.md` Go conventions: receiver named `this`; named slice/return values where applicable; no naked returns; no blank lines at method start/end; struct initializers use field/value pairs; multi-line struct literals close the brace on their own line.
- [ ] Sanity-check the diffs against the three defects in Background: (a) no double encode (dispatcher passes pre-encoded bytes; connector encoder short-circuits on `len(Payload) > 0`), (b) ContentType propagates in-process from serialization → message → dispatch, (c) recovery's `*harness.Message` now flows through the dispatcher to a well-formed broker dispatch (no more `ErrEmptyDispatchTopic`).
- [ ] Confirm package doc comment in `handlers/harness/sqladapter/dispatcher.go` still accurately describes the columns it depends on (`id`, `dispatched`, `type`, `payload`); it has not changed.
