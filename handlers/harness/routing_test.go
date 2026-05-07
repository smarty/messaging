package harness

import (
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestRouterFixture(t *testing.T) {
	gunit.Run(new(RouterFixture), t)
}

type RouterFixture struct {
	*gunit.Fixture
	orchestrator *fakeOrchestrator
	observer     *fakeObserver
	router       *router
}

func (this *RouterFixture) Setup() {
	this.orchestrator = new(fakeOrchestrator)
	this.observer = new(fakeObserver)
	this.router = newRouter(this.orchestrator, this.observer, new(fakeBystander))
}

func (this *RouterFixture) TestExecuteRoutesToRegisteredExecutorAndForwardsBroadcast() {
	var broadcast []any
	this.router.Execute(messageA{value: "yo"}, func(out ...any) {
		broadcast = append(broadcast, out...)
	})

	this.So(this.orchestrator.executed, should.Equal, []any{messageA{value: "yo"}})
	this.So(broadcast, should.Equal, []any{resultA{value: "yo"}})
}

func (this *RouterFixture) TestExecuteAppliesResultToSelfApplicator() {
	this.router.Execute(messageA{value: "yo"}, func(...any) {})

	this.So(this.orchestrator.applied, should.Equal, []any{resultA{value: "yo"}})
}

func (this *RouterFixture) TestExecuteAppliesResultToOtherApplicators() {
	this.router.Execute(messageA{value: "yo"}, func(...any) {})

	this.So(this.observer.applied, should.Equal, []any{resultA{value: "yo"}})
}

func (this *RouterFixture) TestExecuteAppliesEachResultExactlyOncePerApplicator() {
	this.router.Execute(messageA{value: "yo"}, func(...any) {})

	this.So(len(this.orchestrator.applied), should.Equal, 1)
	this.So(len(this.observer.applied), should.Equal, 1)
}

func (this *RouterFixture) TestExecuteUnknownTypeIsNoOp() {
	var broadcast []any
	this.router.Execute(unknownMsg{}, func(out ...any) {
		broadcast = append(broadcast, out...)
	})

	this.So(broadcast, should.BeEmpty)
	this.So(len(this.orchestrator.executed), should.Equal, 0)
}

func (this *RouterFixture) TestExecuteIgnoresMethodsWithBadSignatures() {
	var broadcast []any
	this.router.Execute(bogusExecMsg{}, func(out ...any) {
		broadcast = append(broadcast, out...)
	})

	this.So(broadcast, should.BeEmpty)
	this.So(len(this.orchestrator.executed), should.Equal, 0)
}

func (this *RouterFixture) TestApplyRoutesToAllRegisteredApplicators() {
	this.router.Apply(resultA{value: "yay"})

	this.So(this.orchestrator.applied, should.Equal, []any{resultA{value: "yay"}})
	this.So(this.observer.applied, should.Equal, []any{resultA{value: "yay"}})
}

func (this *RouterFixture) TestApplyIgnoresMethodsWithBadSignatures() {
	this.router.Apply(bogusApplyMsg{})

	this.So(len(this.orchestrator.applied), should.Equal, 0)
	this.So(len(this.observer.applied), should.Equal, 0)
}

func (this *RouterFixture) TestApplyUnknownTypeIsNoOp() {
	this.router.Apply(unknownMsg{})

	this.So(len(this.orchestrator.applied), should.Equal, 0)
}

func (this *RouterFixture) TestExecuteResetsExclusionsBetweenInvocations() {
	this.router.Execute(messageA{value: "first"}, func(...any) {})
	this.router.Execute(messageA{value: "second"}, func(...any) {})

	this.So(this.orchestrator.applied, should.Equal, []any{
		resultA{value: "first"}, resultA{value: "second"},
	})
	this.So(this.observer.applied, should.Equal, []any{
		resultA{value: "first"}, resultA{value: "second"},
	})
}

// TestExecuteResultWithNoSelfApplicator covers the case where an executor produces
// a result whose type it does not apply itself, exercising selfApplicator's nil-return.
func (this *RouterFixture) TestExecuteResultWithNoSelfApplicator() {
	var broadcast []any
	this.router.Execute(messageB{value: "ping"}, func(out ...any) {
		broadcast = append(broadcast, out...)
	})

	this.So(broadcast, should.Equal, []any{resultB{value: "ping"}})
	this.So(this.observer.appliedB, should.Equal, []any{resultB{value: "ping"}})
}

type (
	messageA      struct{ value string }
	resultA       struct{ value string }
	messageB      struct{ value string }
	resultB       struct{ value string }
	unknownMsg    struct{}
	bogusExecMsg  struct{}
	bogusApplyMsg struct{}
)

// fakeOrchestrator satisfies both executor and applicator; its specific Execute*/Apply*
// methods exercise every branch of scan() — both the matching cases and the cases that
// must be filtered out.
type fakeOrchestrator struct {
	executed []any
	applied  []any
}

func (this *fakeOrchestrator) Execute(message any, broadcast func(...any)) {
	switch typed := message.(type) {
	case messageA:
		this.ExecuteMessageA(typed, broadcast)
	case messageB:
		this.ExecuteMessageB(typed, broadcast)
	}
}
func (this *fakeOrchestrator) Apply(message any) {
	switch typed := message.(type) {
	case resultA:
		this.ApplyResultA(typed)
	}
}
func (this *fakeOrchestrator) ExecuteMessageA(msg messageA, broadcast func(...any)) {
	this.executed = append(this.executed, msg)
	broadcast(resultA{value: msg.value})
}
func (this *fakeOrchestrator) ExecuteMessageB(msg messageB, broadcast func(...any)) {
	this.executed = append(this.executed, msg)
	broadcast(resultB{value: msg.value})
}
func (this *fakeOrchestrator) ApplyResultA(msg resultA) {
	this.applied = append(this.applied, msg)
}

func (this *fakeOrchestrator) Execute2() {
	panic("scan() must skip methods with no message argument")
}
func (this *fakeOrchestrator) ExecuteBadReturn(_ bogusExecMsg, _ func(...any)) error {
	panic("scan() must skip methods with non-zero return values")
}
func (this *fakeOrchestrator) ExecuteShortArgs(_ bogusExecMsg) {
	panic("scan() must skip Execute methods that lack the broadcast func")
}
func (this *fakeOrchestrator) ExecuteBadLast(_ bogusExecMsg, _ string) {
	panic("scan() must skip Execute methods whose last arg is not func(...any)")
}
func (this *fakeOrchestrator) ApplyBadReturn(_ bogusApplyMsg) error {
	panic("scan() must skip Apply methods with non-zero return values")
}
func (this *fakeOrchestrator) ApplyExtraArg(_ bogusApplyMsg, _ string) {
	panic("scan() must skip Apply methods with more than one argument")
}
func (this *fakeOrchestrator) Bogus(_ messageA) {
	panic("scan() must skip methods that lack the Execute/Apply prefix")
}

type fakeObserver struct {
	applied  []any
	appliedB []any
}

func (this *fakeObserver) Apply(message any) {
	switch typed := message.(type) {
	case resultA:
		this.ApplyResultA(typed)
	case resultB:
		this.ApplyResultB(typed)
	}
}
func (this *fakeObserver) ApplyResultA(msg resultA) {
	this.applied = append(this.applied, msg)
}
func (this *fakeObserver) ApplyResultB(msg resultB) {
	this.appliedB = append(this.appliedB, msg)
}

type fakeBystander struct{}

func (this *fakeBystander) Hello() {}
