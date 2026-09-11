//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/rabbitmq"
)

// Addresses default to the ports in doc/docker-compose.integration.yml, chosen
// so this stack can run alongside sibling repositories' stacks on one dev box.
// CI can point the same tests elsewhere through the environment.
var (
	brokerAddress     = envOr("INTEGRATION_RABBITMQ_ADDR", "amqp://guest:guest@127.0.0.1:5678/")
	managementAddress = envOr("INTEGRATION_RABBITMQ_MANAGEMENT", "http://guest:guest@127.0.0.1:15678")
)

func envOr(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

// TestMain waits for the broker's AMQP and management listeners, so a freshly
// started container cannot fail the first test with a refused connection.
func TestMain(m *testing.M) {
	if err := awaitBroker(time.Second * 60); err != nil {
		fmt.Fprintf(os.Stderr, "integration: broker not ready: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
func awaitBroker(timeout time.Duration) (err error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err = probeBroker(); err == nil {
			return nil
		}
		time.Sleep(time.Millisecond * 250)
	}
	return err
}
func probeBroker() error {
	client := &http.Client{Timeout: 2 * time.Second}
	overview := newManagement()
	request, _ := http.NewRequest(http.MethodGet, overview.base+"/api/overview", nil)
	request.SetBasicAuth(overview.user, overview.pass)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("management API answered %d", response.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connector := rabbitmq.New(rabbitmq.Options.Address(brokerAddress), rabbitmq.Options.BrokerTimeout(2*time.Second))
	defer func() { _ = connector.Close() }()
	connection, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	return connection.Close()
}

var sequence atomic.Int64

// uniqueName keeps every test's topology apart so runs cannot pollute each other.
func uniqueName(kind string) string {
	return fmt.Sprintf("messaging-it-%s-%d-%d", kind, time.Now().UnixNano(), sequence.Add(1))
}

func waitFor(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond * 50)
	}
	return condition()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// management is a minimal client for the RabbitMQ HTTP API, used to declare
// and inspect topology and to force the broker-side events the library must
// react to (closing connections, deleting queues).
type management struct {
	base   string
	user   string
	pass   string
	client *http.Client
}

func newManagement() *management {
	parsed, err := url.Parse(managementAddress)
	if err != nil {
		panic(err)
	}
	user := parsed.User.Username()
	pass, _ := parsed.User.Password()
	parsed.User = nil
	return &management{base: strings.TrimRight(parsed.String(), "/"), user: user, pass: pass, client: &http.Client{Timeout: 10 * time.Second}}
}
func (this *management) do(method, path string, body any) (int, []byte) {
	var payload []byte
	if body != nil {
		payload, _ = json.Marshal(body)
	}
	request, err := http.NewRequest(method, this.base+path, bytes.NewReader(payload))
	if err != nil {
		panic(err)
	}
	request.SetBasicAuth(this.user, this.pass)
	request.Header.Set("Content-Type", "application/json")
	response, err := this.client.Do(request)
	if err != nil {
		panic(fmt.Sprintf("management %s %s: %v", method, path, err))
	}
	defer func() { _ = response.Body.Close() }()
	var buffer bytes.Buffer
	_, _ = buffer.ReadFrom(response.Body)
	return response.StatusCode, buffer.Bytes()
}
func (this *management) DeclareExchange(name string) {
	this.do(http.MethodPut, "/api/exchanges/%2F/"+name, map[string]any{"type": "fanout", "durable": true})
}
func (this *management) DeclareQueue(name string) {
	this.do(http.MethodPut, "/api/queues/%2F/"+name, map[string]any{"durable": true})
}
func (this *management) DeclareQuorumQueue(name string) {
	this.do(http.MethodPut, "/api/queues/%2F/"+name, map[string]any{"durable": true, "arguments": map[string]any{"x-queue-type": "quorum"}})
}

// Nodes reports each cluster member and whether it is running, as seen by the
// node the management address points at.
func (this *management) Nodes() map[string]bool {
	_, body := this.do(http.MethodGet, "/api/nodes", nil)
	var nodes []struct {
		Name    string `json:"name"`
		Running bool   `json:"running"`
	}
	_ = json.Unmarshal(body, &nodes)
	result := map[string]bool{}
	for _, node := range nodes {
		result[node.Name] = node.Running
	}
	return result
}
func (this *management) RunningNodes() (count int) {
	for _, running := range this.Nodes() {
		if running {
			count++
		}
	}
	return count
}

// QuorumLeader returns the node the management API lists as leading a quorum
// queue, or "" when the queue does not exist. The field goes stale while the
// queue is in minority, so use QuorumOnline to detect minority.
func (this *management) QuorumLeader(name string) string {
	status, body := this.do(http.MethodGet, "/api/queues/%2F/"+name, nil)
	if status != http.StatusOK {
		return ""
	}
	var queue struct {
		Leader string `json:"leader"`
	}
	_ = json.Unmarshal(body, &queue)
	return queue.Leader
}

// QuorumStatus returns the raw membership fields of a quorum queue, for
// failure output.
func (this *management) QuorumStatus(name string) string {
	_, body := this.do(http.MethodGet, "/api/queues/%2F/"+name, nil)
	var queue struct {
		State   string   `json:"state"`
		Leader  string   `json:"leader"`
		Members []string `json:"members"`
		Online  []string `json:"online"`
	}
	_ = json.Unmarshal(body, &queue)
	return fmt.Sprintf("state=%s leader=%s members=%v online=%v", queue.State, queue.Leader, queue.Members, queue.Online)
}

// QuorumOnline returns how many of a quorum queue's members are online.
func (this *management) QuorumOnline(name string) int {
	status, body := this.do(http.MethodGet, "/api/queues/%2F/"+name, nil)
	if status != http.StatusOK {
		return -1
	}
	var queue struct {
		Online []string `json:"online"`
	}
	_ = json.Unmarshal(body, &queue)
	return len(queue.Online)
}
func (this *management) Bind(queue, exchange string) {
	this.do(http.MethodPost, "/api/bindings/%2F/e/"+exchange+"/q/"+queue, map[string]any{"routing_key": ""})
}
func (this *management) DeleteQueue(name string) {
	this.do(http.MethodDelete, "/api/queues/%2F/"+name, nil)
}
func (this *management) DeleteExchange(name string) {
	this.do(http.MethodDelete, "/api/exchanges/%2F/"+name, nil)
}

// QueueDepth returns the message count, or -1 when the queue does not exist.
func (this *management) QueueDepth(name string) int {
	status, body := this.do(http.MethodGet, "/api/queues/%2F/"+name, nil)
	if status != http.StatusOK {
		return -1
	}
	var queue struct {
		Messages int `json:"messages"`
	}
	_ = json.Unmarshal(body, &queue)
	return queue.Messages
}

// Connections lists the names of the client connections the broker knows
// about. The management plugin learns of a new connection on its stats
// interval, so callers poll.
func (this *management) Connections() (names []string) {
	_, body := this.do(http.MethodGet, "/api/connections", nil)
	var connections []struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(body, &connections)
	for _, connection := range connections {
		names = append(names, connection.Name)
	}
	return names
}
func (this *management) CloseConnection(name string) {
	this.do(http.MethodDelete, "/api/connections/"+url.PathEscape(name), nil)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// capturingLog is safe for the library's background goroutines.
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
func (this *capturingLog) Contains(substring string) bool {
	return strings.Contains(this.String(), substring)
}

// rabbitMonitor records rabbitmq monitor callbacks by name.
type rabbitMonitor struct {
	mutex sync.Mutex
	calls []string
}

func (this *rabbitMonitor) record(call string) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.calls = append(this.calls, call)
}
func (this *rabbitMonitor) Count(call string) (count int) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for _, recorded := range this.calls {
		if recorded == call {
			count++
		}
	}
	return count
}
func (this *rabbitMonitor) ConnectionOpened(err error) {
	if err == nil {
		this.record("opened")
	} else {
		this.record("open-failed")
	}
}
func (this *rabbitMonitor) ConnectionClosed()                      { this.record("closed") }
func (this *rabbitMonitor) ConnectionBlocked(string)               { this.record("blocked") }
func (this *rabbitMonitor) ConnectionUnblocked()                   { this.record("unblocked") }
func (this *rabbitMonitor) DispatchPublished()                     { this.record("published") }
func (this *rabbitMonitor) DeliveryReceived()                      { this.record("received") }
func (this *rabbitMonitor) DeliveryAcknowledged(_ uint16, _ error) { this.record("acknowledged") }
func (this *rabbitMonitor) TransactionCommitted(err error) {
	if err == nil {
		this.record("committed")
	} else {
		this.record("commit-failed")
	}
}
func (this *rabbitMonitor) TransactionRolledBack(error) { this.record("rolled-back") }

// collectingHandler gathers every message it is handed.
type collectingHandler struct {
	mutex    sync.Mutex
	messages []any
}

func (this *collectingHandler) Handle(_ context.Context, messages ...any) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.messages = append(this.messages, messages...)
}
func (this *collectingHandler) Messages() []any {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return append([]any(nil), this.messages...)
}

func publish(ctx context.Context, connector messaging.Connector, dispatches ...messaging.Dispatch) error {
	connection, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	writer, err := connection.CommitWriter(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = writer.Close() }()
	if _, err = writer.Write(ctx, dispatches...); err != nil {
		return err
	}
	return writer.Commit()
}

// drain consumes everything currently on the queue, acknowledging as it goes,
// and returns the message types in order. It stops after quiet passes with no
// delivery. Consuming is deterministic where the management API's message
// counts are not: those refresh on the plugin's stats interval.
func drain(ctx context.Context, connector messaging.Connector, queue string, quiet time.Duration) (types []string, err error) {
	connection, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = connection.Close() }()
	reader, err := connection.Reader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	stream, err := reader.Stream(ctx, messaging.StreamConfig{StreamName: queue, BufferCapacity: 16})
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()
	for {
		readCtx, cancel := context.WithTimeout(ctx, quiet)
		var delivery messaging.Delivery
		readErr := stream.Read(readCtx, &delivery)
		cancel()
		if readErr != nil {
			return types, nil
		}
		types = append(types, delivery.MessageType)
		if err = stream.Acknowledge(ctx, delivery); err != nil {
			return types, err
		}
	}
}
