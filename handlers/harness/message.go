package harness

import "bytes"

type Message struct {
	// ID represents the unique ID of this message and its sequential place within a larger stream.
	ID uint64

	// Type is the name registered for the (Go) type of the Value.
	// (i.e. 'subscription:subscription-renewed-v2').
	Type string

	// Value contains the in-memory Go message structure.
	Value any

	// Content contains the serialized representation of the Go Value.
	Content *bytes.Buffer

	// ContentType identifies the serialization method employed to represent the Content
	// (i.e. 'application/json; charset=utf-8').
	ContentType string
}
