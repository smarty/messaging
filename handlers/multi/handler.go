package multi

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type handler []messaging.Handler

func New(handlers ...messaging.Handler) messaging.Handler {
	return handler(handlers)
}

func (this handler) Handle(ctx context.Context, messages ...interface{}) {
	for _, handler := range this {
		handler.Handle(ctx, messages...)
	}
}
