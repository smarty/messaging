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
	httpEntrypoint interface {
		admitter
		awaiter
	}
)

// admission refuses overloaded requests before the wrapped handler runs,
// writing an inline 503. Wrap each mutating route with it.
func admission(handler admitter, inner http.Handler) http.Handler {
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

// HTTPAdapter adapts the entrypoint Handler for use by an http.Handler.
type HTTPAdapter interface {
	// Handler is what the user-supplied http.Handler will invoke
	messaging.Handler

	// HTTPHandler wraps the user-supplied http.Handler with an admission check.
	HTTPHandler(inner http.Handler) (wrapped http.Handler)
}

type httpAdapter struct {
	target httpEntrypoint
}

func newHTTPAdapter(target httpEntrypoint) *httpAdapter {
	return &httpAdapter{target}
}

func (this *httpAdapter) HTTPHandler(inner http.Handler) http.Handler {
	return admission(this.target, inner)
}
func (this *httpAdapter) Handle(ctx context.Context, messages ...any) {
	for _, message := range messages {
		this.target.await(ctx, message)
	}
}
