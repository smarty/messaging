package messaging

type (
	MessageBroker interface {
		Connect() error
		Disconnect()

		OpenReader(queue string, bindings ...string) Reader
		OpenTransientReader(bindings []string) Reader

		OpenWriter() Writer
		OpenTransactionalWriter() CommitWriter
	}

	Reader interface {
		Listen()
		Close()

		Deliveries() <-chan Delivery
		Acknowledgements() chan<- interface{}
	}

	Writer interface {
		Write(Dispatch) error
		Close()
	}

	CommitWriter interface {
		Writer
		Commit() error
	}
)

type Serializer interface {
	Serialize(interface{}) ([]byte, error)
	ContentType() string
	ContentEncoding() string
}

type TypeDiscovery interface {
	Discover(interface{}) (string, error)
}
