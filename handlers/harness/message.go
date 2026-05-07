package harness

import (
	"bytes"
	"time"
)

// TODO: unexport?

type Message struct {
	// ID represents the unique ID of this message and its sequential place within a larger stream.
	ID uint64

	// Value contains the in-memory Go message structure.
	Value any

	// Type is the name registered for the (Go) type of the Value.
	// (i.e. 'subscription:subscription-renewed-v2').
	Type string

	// Content contains the serialized representation of the Go Value.
	Content *bytes.Buffer

	// ContentType identifies the serialization method employed to represent the Content
	// (i.e. 'application/json; charset=utf-8').
	ContentType string

	// ContentEncoding identifies the encoding of the Content
	// (i.e. 'gzip').
	ContentEncoding string

	// Stored represents the instant the Type, Content, ContentType, and ContentEncoding
	// were durably stored (a critically important step in processing--if it's not stored, it didn't happen).
	Stored time.Time

	// Dispatched represents the instant the Type, Content, ContentType, and ContentEncoding
	// were published to the external messaging system (if configured).
	Dispatched time.Time
}
