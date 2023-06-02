package streaming

import (
	"context"
	"io"
	"sync"

	"github.com/smarty/messaging/v3"
)

type connectionPool interface {
	Active(context.Context) (messaging.Connection, error)
	Dispose(messaging.Connection)
	io.Closer
}

type defaultConnectionPool struct {
	mutex      sync.Mutex
	connector  messaging.Connector
	connection messaging.Connection
}

func newConnectionPool(connector messaging.Connector) connectionPool {
	return &defaultConnectionPool{connector: connector}
}

func (this *defaultConnectionPool) Active(ctx context.Context) (_ messaging.Connection, err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.connection != nil {
		return this.connection, nil
	}

	this.connection, err = this.connector.Connect(ctx)
	return this.connection, err
}
func (this *defaultConnectionPool) Dispose(connection messaging.Connection) {
	if connection == nil {
		return
	}

	_ = connection.Close()

	this.mutex.Lock()
	this.mutex.Unlock()

	if this.connection == connection {
		this.connection = nil
	}
}

func (this *defaultConnectionPool) Close() error {
	this.mutex.Lock()
	connection := this.connection
	this.mutex.Unlock()

	this.Dispose(connection)

	return nil
}
