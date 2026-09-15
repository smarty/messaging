package streaming

import (
	"fmt"
	"sync"
	"time"
)

// capturingMonitor records each callback as one line, safe for concurrent workers.
type capturingMonitor struct {
	mutex sync.Mutex
	calls []string
}

func (this *capturingMonitor) record(format string, args ...any) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.calls = append(this.calls, fmt.Sprintf(format, args...))
}
func (this *capturingMonitor) Calls() []string {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return append([]string(nil), this.calls...)
}

func (this *capturingMonitor) StreamOpened(streamName string, err error) {
	this.record("opened:%s:%v", streamName, err)
}
func (this *capturingMonitor) StreamClosed(streamName string) {
	this.record("closed:%s", streamName)
}
func (this *capturingMonitor) BatchHandled(streamName string, count int, duration time.Duration) {
	this.record("handled:%s:%d:%s", streamName, count, duration)
}
func (this *capturingMonitor) BatchAcknowledged(streamName string, count int, err error) {
	this.record("acknowledged:%s:%d:%v", streamName, count, err)
}
func (this *capturingMonitor) ShutdownForced(streamName string) {
	this.record("forced:%s", streamName)
}
