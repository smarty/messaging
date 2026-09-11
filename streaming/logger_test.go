package streaming

import (
	"fmt"
	"strings"
	"sync"
)

// capturingLog is safe for the concurrent Printf calls that the manager's
// per-subscription goroutines and the worker's reader goroutine make.
type capturingLog struct {
	mutex sync.Mutex
	lines []string
}

func (this *capturingLog) Printf(format string, args ...any) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.lines = append(this.lines, fmt.Sprintf(format, args...))
}
func (this *capturingLog) String() string {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return strings.Join(this.lines, "\n")
}
