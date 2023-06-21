package retry

import (
	"time"

	"github.com/smarty/messaging/v4"
)

func New(inner messaging.Handler, options ...option) messaging.Handler {
	this := handler{Handler: inner, immediate: map[interface{}]struct{}{}}

	for _, option := range Options.defaults(options...) {
		option(&this)
	}

	return this
}

var Options singleton

type singleton struct{}
type option func(*handler)

func (singleton) Timeout(value time.Duration) option {
	return func(this *handler) { this.timeout = value }
}
func (singleton) MaxAttempts(value uint32) option {
	return func(this *handler) { this.maxAttempts = int(value) }
}
func (singleton) ImmediateRetry(value ...interface{}) option {
	return func(this *handler) {
		for _, err := range value {
			this.immediate[err] = struct{}{}
		}
	}
}
func (singleton) Logger(value logger) option {
	return func(this *handler) { this.logger = value }
}
func (singleton) Monitor(value monitor) option {
	return func(this *handler) { this.monitor = value }
}
func (singleton) LogStackTrace(value bool) option {
	return func(this *handler) { this.stackTrace = value }
}

func (singleton) defaults(options ...option) []option {
	const defaultRetryTimeout = time.Second * 5
	const defaultMaxAttempts = 1<<32 - 1
	const defaultLogStackTrace = true
	var defaultLogger = nop{}
	var defaultMonitor = nop{}

	return append([]option{
		Options.Timeout(defaultRetryTimeout),
		Options.MaxAttempts(defaultMaxAttempts),
		Options.LogStackTrace(defaultLogStackTrace),
		Options.Logger(defaultLogger),
		Options.Monitor(defaultMonitor),
	}, options...)
}

type nop struct{}

func (nop) Printf(_ string, _ ...interface{}) {}

func (nop) HandleAttempted(_ int, _ interface{}) {}
