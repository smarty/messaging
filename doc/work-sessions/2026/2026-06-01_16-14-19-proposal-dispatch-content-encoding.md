---
name: Stop double-encoding payloads across dispatch and recovery
description: Eliminate the redundant second serialization in the sqladapter Dispatcher, propagate ContentType through the harness Message and the durable row, and make recovery a faithful re-dispatch (not a JSON-shaped guess) — closing the TODO at handlers/harness/sqladapter/dispatcher.go:58 and the related content-type mishandling on the recovery path.
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
default JSON serializer happens to make the bug benign) and they are masked by
the unit-test stub connector — but they are real, and recovery against a real
RabbitMQ writer is broken today.

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
field exists, the schema row has nowhere to store it, and the dispatcher never
reads it.

This is fine *today* because `dispatcher.go` re-encodes via the connector
(Defect 1), so the connector reapplies its own `ContentType()`. The moment we
fix Defect 1 by passing pre-encoded bytes through, we have to know what the
content type is — and the harness never recorded it.

### Defect 3: recovery silently lies about content type, and is broken against a real broker

`recovery.go:42-47` builds a `harness.Message` from the row:

```go
messages = append(messages, &harness.Message{
    ID:          id,
    Type:        typeName,
    Content:     bytes.NewBuffer(payload),
    ContentType: "application/json",   // <-- hardcoded
})
```

`Value` is left nil. The hardcoded `"application/json"` is read by no one today
(see Defect 2: nothing in dispatch reads `Message.ContentType`), so recovery
"works" by coincidence whenever the configured serializer is JSON. Plug in a
gob/proto/msgpack serializer and the stored bytes are no longer JSON, but the
recovery path still labels them `"application/json"`. Today that label is
ignored, but as soon as we fix Defects 1 and 2 to *use* the recorded content
type, this hardcode becomes a live mislabel.

There is also a worse, present-tense failure: with `Value == nil` and
`Payload == nil`, the connector's encoder takes its `dispatch.Message == nil`
early-return (`dispatch_encoder.go:31`) and **never sets `Topic` /
`MessageType` / `ContentType`**. The RabbitMQ writer then rejects with
`ErrEmptyDispatchTopic` (`rabbitmq/writer.go:38`). So in the only production
configuration that uses `topicFromMessageType=true` (the encoder derives Topic
from MessageType), recovery can't publish a single row. The current dispatcher
unit tests don't catch this because the stub connector
(`dispatcher_test.go:111`) ignores the topic field entirely.

### Why fix all three together

The three defects are one defect viewed from three sides: the harness owns the
serialization, but the harness throws away two of the three things it learned
(`ContentType`, the encoded bytes themselves) and the connector re-derives
them. Recovery, which has only what was persisted, is the path that *must*
carry the ContentType through, and it can't because the harness never wrote
it down. Fixing the dispatcher TODO without fixing the schema/recovery makes
recovery worse, not better. Fixing recovery without fixing the dispatcher TODO
leaves the double-encode in place. They go together.

## Approach

We pick the "pass-through" arm of the TODO: the harness Serialization stage
remains the single source of truth for encoded bytes and content type; the
sqladapter Dispatcher hands those pre-encoded bytes to the connector via
`Dispatch.Payload` / `MessageType` / `ContentType`; the connector encoder
short-circuits because `Payload` is non-empty. Recovery does the same
pass-through using the row's stored `type` and `content_type`.

This is the smaller, safer arm. The alternative (delete the harness
Serialization stage entirely and let the connector own all encoding) would
require persistence to also call the connector's encoder, would entangle the
harness with the connector, and would lose the content-type-known-at-store-
time invariant that recovery depends on. Rejected on those grounds.

### The four changes

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

**Change B (`message.go` schema + `Writer`):** persist `content_type`
alongside `type` and `payload`. Schema migration:

```sql
ALTER TABLE Messages ADD COLUMN content_type varchar(128) NOT NULL DEFAULT '';
```

(`varchar(128)` comfortably holds e.g. `application/json; charset=utf-8`.) The
default lets existing rows survive; recovery will treat `''` as "use legacy
default" — see Recovery below. Update `doc/mysql/schema.sql` to include the
column.

`sqladapter/writer.go:67` becomes:

```go
statement.WriteString(`INSERT INTO Messages (type, content_type, payload) VALUES `)
// args: (message.Type, message.ContentType, message.Content.Bytes())
```

`(?, ?, ?)` per row.

CORRECTION: Unfortunately, we are not going to make any changes to the schema of the Messages table. This table has over 50 million events. When writing/reading from this table we currently assume json. We should still set the content-type whenever dispatching.

**Change C (`sqladapter/dispatcher.go`):** stop passing `Value`; pass the
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

**Change D (`sqladapter/recovery.go`):** scan `content_type` and propagate it.
Drop the hardcoded `"application/json"`:

```sql
SELECT id, type, content_type, payload
  FROM Messages
 WHERE dispatched IS NULL
 ORDER BY id
```

```go
messages = append(messages, &harness.Message{
    ID:          id,
    Type:        typeName,
    Content:     bytes.NewBuffer(payload),
    ContentType: contentType, // from the row, not hardcoded
})
```

Rows that pre-date the migration arrive with `content_type = ''`. Two options:

1. Treat empty as `"application/json"` (today's hardcoded value) — preserves
   today's coincidental behavior for in-flight rows during a deploy.
2. Refuse to recover rows with empty `content_type` — surfaces the migration
   gap loudly.

We pick **(1)** with a one-line `logger.Printf("[WARN] …")` per such row, so
the migration is observable but not destructive. The default applies only to
rows written before this change is deployed; once it's in, every new row
carries its real content type.

CORRECTION: we aren't going to modify the schema of the Messages table. Assume application/json when reading.

### Files created / modified

| File                                                  | Change                                                                                                  |
|-------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `doc/mysql/schema.sql`                                | Add `content_type varchar(128) NOT NULL DEFAULT ''` column to `Messages`.                               |
| `handlers/harness/contracts.go`                       | Extend internal `serializer` interface with `ContentType() string`.                                     |
| `handlers/harness/02_serialization.go`                | On success, set `message.ContentType = this.serializer.ContentType()`.                                  |
| `handlers/harness/02_serialization_test.go`           | Update fixture serializer to satisfy `ContentType() string`; assert ContentType propagates.             |
| `handlers/harness/config.go`                          | `nop.ContentType() string { return "" }`.                                                               |
| `handlers/harness/sqladapter/writer.go`               | Insert `content_type` column from `message.ContentType`.                                                |
| `handlers/harness/sqladapter/writer_test.go`          | Assert the persisted `content_type` column matches input; widen `serializedMessage` helper accordingly. |
| `handlers/harness/sqladapter/dispatcher.go`           | Pass `Payload`/`MessageType`/`ContentType`/`Topic` from `*harness.Message`; remove TODO.                |
| `handlers/harness/sqladapter/dispatcher_test.go`      | Assert published Dispatch carries Payload/MessageType/ContentType/Topic; tighten stub connector.        |
| `handlers/harness/sqladapter/recovery.go`             | Select `content_type`, propagate it; legacy `''` → warn + default to `"application/json"`.              |
| `handlers/harness/sqladapter/recovery_test.go`        | Seed rows with explicit `content_type`; assert the recovered Dispatch carries that content type.        |

No production-side migration tooling is in scope; the column has a default, so
deployments add the column with `ALTER TABLE` ahead of the new binary and the
binary tolerates the legacy default for any rows that race.

### Alternatives considered

- **Drop the harness Serialization stage; let the connector serialize.** The
  other arm of the TODO. Rejected: the persistence stage would have to call
  the connector's encoder before INSERT (otherwise we lose the
  content-type-at-store-time invariant), which entangles the harness with the
  serialization-connector and changes the order of failure (a connector
  encoding bug would now poison persistence rather than just dispatch).
  Pass-through is the smaller change.

- **Skip the schema migration; have recovery re-derive content type from a
  registered serializer.** Rejected: recovery would have to be told which
  serializer was in effect *when the row was written*, which is unknowable
  after a serializer-config change. Storing the content type next to the bytes
  is the only way to guarantee recovery re-publishes what was originally
  persisted.

- **Have the dispatcher consult the connector's `topicFromMessageType` setting
  to decide whether to populate Topic.** Rejected: the sqladapter dispatcher
  has no handle to that config, the configuration is owned by a different
  package, and the dispatcher's contract is "publish what was persisted." For
  this pipeline, Topic = Type is the rule; encoding it explicitly in the
  dispatcher makes the rule visible at the call site instead of hidden in
  another package's option flag.

## Trade-offs & Risks

- **Schema migration is required.** Existing deployments must `ALTER TABLE`
  before rolling the new binary. The new binary tolerates the legacy default
  on read (warns and falls back to `"application/json"`); old binaries
  tolerate the new column (they don't reference it). So the deploy order is:
  (1) ALTER TABLE in prod, (2) deploy new binary. There is a window where
  rows might be written by an old binary into a schema with the new column —
  the column has `DEFAULT ''`, so the INSERT still succeeds; those rows
  recover via the legacy fallback path.

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
  (`dispatch_encoder.go:31`), but we are now relying on it. A short test in
  `serialization/dispatch_encoder_test.go` already asserts this (`TestWhen-
  DispatchAlreadyContainsSerializedPayload_Nop`); we'll cross-reference it in
  a comment but not duplicate it.

- **Recovery's legacy-row warning is one log line per row.** For a migration
  with thousands of un-dispatched rows, that could be loud. We could rate-
  limit, but the situation is itself a one-off (the cutover deploy); a one-
  time spike of warnings is the right signal. We will *not* rate-limit.

- **The dispatcher unit test that previously asserted `published[0].Message`
  must change** — the stub connector observes `Message` today, but after the
  change the dispatcher passes `Payload` and leaves `Message` nil. The test
  becomes: assert `Payload` is the persisted bytes, `MessageType` is the
  registered name, `ContentType` is what the harness recorded, `Topic` equals
  MessageType, `Durable` is true.

- **Coverage and shape of behavior tests are otherwise unchanged.** No new
  goroutine semantics, no concurrency-shape changes; this is a pure data-flow
  refactor with one schema column added.

## Implementation Checklist

### Phase 1: ContentType on the internal serializer interface (red → green)

- [ ] Extend `02_serialization_test.go` fixture: add `ContentType() string { return "test/content-type" }` and a new test `TestSerializesEachResultValueIntoContent_PopulatesContentTypeOnSuccess` asserting `units[0].results[0].ContentType == "test/content-type"`.
- [ ] Run tests, confirm failure (compile error: `*SerializationFixture` does not satisfy `serializer` once we extend the interface — wait, we extend the interface in the next step, so the failure is the assertion: ContentType is empty).
- [ ] Extend `serializer` interface in `handlers/harness/contracts.go` to include `ContentType() string`.
- [ ] Add `ContentType() string { return "" }` to `nop` in `config.go` so the default still satisfies the interface.
- [ ] In `02_serialization.go`, on the success branch, set `message.ContentType = this.serializer.ContentType()`. Leave the fallback branch's existing `"go fmt.Sprintf(%#v)"` assignment alone.
- [ ] Run tests, confirm green. Verify the fallback test (`TestSerializerErrorIsTracked_FallbackToFmtSprintfEncoding`) still asserts the `"go fmt.Sprintf(%#v)"` ContentType.

### Phase 2: Persist `content_type` (red → green)

- [ ] Update `doc/mysql/schema.sql` to include `content_type varchar(128) NOT NULL DEFAULT ''` and a corresponding column in `CREATE TABLE Messages`.
- [ ] Update `testdb_test.go` (the integration-test schema bootstrap) to match.
- [ ] Modify `WriterFixture.serializedMessage` (in `writer_test.go`) to set a non-empty `ContentType` (e.g. `"application/json"`); add `TestWrite_PersistsContentTypeColumn` asserting the row's `content_type` equals the input `ContentType`.
- [ ] Run tests, confirm failure (the column is missing in the INSERT — Go SQL error or assertion fails).
- [ ] In `sqladapter/writer.go`, change the INSERT to `(type, content_type, payload) VALUES (?, ?, ?)`; append `message.ContentType` between `message.Type` and `message.Content.Bytes()`. Update the `args` capacity hint accordingly.
- [ ] Run tests, confirm green. Verify pre-existing writer tests still pass (no payload behavior change).

### Phase 3: Dispatcher pass-through (red → green)

- [ ] Add `TestDispatch_PublishesPreEncodedPayloadAndMetadata` to `dispatcher_test.go`: seed a message via the existing `seedMessage` (extended to carry `Type`, `ContentType`, and pre-encoded `Content`); assert the recorded `messaging.Dispatch` has `Payload == message.Content.Bytes()`, `MessageType == message.Type`, `ContentType == message.ContentType`, `Topic == message.Type`, `Durable == true`, and `Message == nil`.
- [ ] Run tests, confirm failure (today the dispatcher sends `Message: message.Value` and leaves Payload/MessageType/ContentType/Topic blank).
- [ ] Edit `sqladapter/dispatcher.go:55-71`:
    - Drop the TODO comment block.
    - Change the `messaging.Dispatch` literal to `{Durable: true, MessageType: message.Type, ContentType: message.ContentType, Payload: message.Content.Bytes(), Topic: message.Type}`.
    - Add a one-line comment that Topic = Type because the connector encoder is short-circuited by the pre-populated Payload.
- [ ] Run tests, confirm green. Update or delete the prior `TestDispatch_PublishesAndMarksDispatched` assertion that referenced `published[0].Message` so it asserts on Payload/MessageType/ContentType/Topic instead.
- [ ] Confirm `TestDispatch_PublishFails_ReturnsErrorWithoutMarkingDispatched` and `TestDispatch_NoMessages_NoOp` still pass.

### Phase 4: Recovery propagates ContentType (red → green)

- [ ] Update `recovery_test.go` `seedUndispatched` helper to take a `contentType string` parameter and INSERT it; thread the value through existing tests with `"application/json"`.
- [ ] Add `TestRecover_PropagatesContentTypeFromRow`: seed two rows with different `content_type` values (e.g. `"application/json"`, `"application/x-protobuf"`); assert the resulting `messaging.Dispatch` records carry those exact ContentType values.
- [ ] Add `TestRecover_LegacyRowWithEmptyContentType_DefaultsToJSONAndWarns`: seed a row with `content_type = ''`; assert the resulting Dispatch's `ContentType == "application/json"` and a warning was logged. (Use a capturing logger.)
- [ ] Run tests, confirm failure (recovery currently hardcodes `"application/json"` for every row regardless of column).
- [ ] Edit `sqladapter/recovery.go`:
    - Add `content_type` to the SELECT column list and to `rows.Scan`.
    - Drop the hardcoded ContentType.
    - When the scanned content type is `""`, set ContentType to `"application/json"` and `logger.Printf("[WARN] Recovering legacy row id=%d with empty content_type; defaulting to application/json", id)`.
- [ ] Run tests, confirm green. Confirm `TestRecover_NoOrphans_NoOp`, `TestRecover_DispatchesUndispatchedRowsInIDOrder`, `TestRecover_RowsExceedBatchSize_FlushesInBatchesAndDispatchesAll`, and `TestRecover_RowCountIsMultipleOfBatchSize_FlushesUniformBatches` all still pass with the updated seed helper.

### Phase 5: Full verification

- [ ] Run `make test` (fmt, vet, `-race`, coverage). Confirm green and that `handlers/harness` and `handlers/harness/sqladapter` coverage have not regressed.
- [ ] Re-read the diff against the `CLAUDE.md` Go conventions: receiver named `this`; named slice/return values where applicable; no naked returns; no blank lines at method start/end; struct initializers use field/value pairs; multi-line struct literals close the brace on their own line.
- [ ] Sanity-check the four diffs against the three defects in Background: (a) no double encode (dispatcher passes pre-encoded bytes; connector encoder short-circuits on `len(Payload) > 0`), (b) ContentType propagates through every stage end-to-end (serialization → message → row → recovery → dispatch), (c) recovery no longer hardcodes `"application/json"` for new rows.
- [ ] Confirm package doc comment in `handlers/harness/sqladapter/dispatcher.go` still accurately describes the columns it depends on (`type`, `content_type`, `payload`, `dispatched`, `id`); update if needed.
