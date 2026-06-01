package harness

import (
	"context"
	"net/http"

	"github.com/smarty/messaging/v3"
)

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
func Admission(handler admitter, inner http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if handler.admit() {
			inner.ServeHTTP(response, request)
			return
		}
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		response.Header().Set("Retry-After", "1")
		response.WriteHeader(http.StatusServiceUnavailable)
		_, _ = response.Write(shedResponseBody)
	})
}

var shedResponseBody = []byte(`{"errors":[{"message":"service overloaded"}]}`)

// AsHTTPHandler adapts the void, context-honoring await to the
// messaging.Handler the HTTP shells already depend on, so no shell (and no
// shell test) changes.
func AsHTTPHandler(handler awaiter) messaging.Handler {
	return &httpAdapter{target: handler}
}

type httpAdapter struct {
	target awaiter
}

func (this *httpAdapter) Handle(ctx context.Context, messages ...any) {
	for _, message := range messages {
		this.target.await(ctx, message)
	}
}
