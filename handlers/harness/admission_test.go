package harness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/smarty/gunit/v2"
	"github.com/smarty/gunit/v2/assert/should"
)

func TestAdmissionFixture(t *testing.T) {
	gunit.Run(new(AdmissionFixture), t)
}

type AdmissionFixture struct {
	*gunit.Fixture
	ctx context.Context
}

func (this *AdmissionFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
}

type fakeAwaiter struct {
	ctxs     []context.Context
	messages []any
}

func (this *fakeAwaiter) Handle(context.Context, ...any) {}
func (this *fakeAwaiter) await(ctx context.Context, message any) {
	this.ctxs = append(this.ctxs, ctx)
	this.messages = append(this.messages, message)
}

func (this *AdmissionFixture) TestAsHTTPHandler_ForwardsSingleMessageToAwait() {
	fake := &fakeAwaiter{}
	AsHTTPHandler(fake).Handle(this.ctx, "x")
	this.So(fake.messages, should.Equal, []any{"x"})
	this.So(fake.ctxs, should.HaveLength, 1)
	this.So(fake.ctxs[0].Value("testing"), should.Equal, this.Name())
}

func (this *AdmissionFixture) TestAsHTTPHandler_ForwardsEachMessageInOrder() {
	fake := &fakeAwaiter{}
	AsHTTPHandler(fake).Handle(this.ctx, "a", "b")
	this.So(fake.messages, should.Equal, []any{"a", "b"})
}

type fakeAdmitter struct {
	allow bool
}

func (this *fakeAdmitter) Handle(context.Context, ...any) {}
func (this *fakeAdmitter) admit() bool                    { return this.allow }

func (this *AdmissionFixture) TestAdmission_PassesThroughWhenAdmitted() {
	var ran bool
	inner := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		ran = true
		response.WriteHeader(http.StatusTeapot)
		_, _ = response.Write([]byte("inner"))
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/admin/orders", nil)
	Admission(&fakeAdmitter{allow: true}, inner).ServeHTTP(recorder, request)

	this.So(ran, should.BeTrue)
	this.So(recorder.Code, should.Equal, http.StatusTeapot)
	this.So(recorder.Body.String(), should.Equal, "inner")
}

func (this *AdmissionFixture) TestAdmission_Writes503WhenRejected() {
	var ran bool
	inner := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { ran = true })

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/admin/orders", nil)
	Admission(&fakeAdmitter{allow: false}, inner).ServeHTTP(recorder, request)

	this.So(ran, should.BeFalse)
	this.So(recorder.Code, should.Equal, http.StatusServiceUnavailable)
	this.So(recorder.Header().Get("Content-Type"), should.Equal, "application/json; charset=utf-8")
	this.So(recorder.Header().Get("Retry-After"), should.Equal, "1")
	this.So(recorder.Body.String(), should.Equal, `{"errors":[{"message":"service overloaded"}]}`)
}
