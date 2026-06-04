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

	allow    bool
	ctxs     []context.Context
	messages []any
}

func (this *AdmissionFixture) Setup() {
	this.ctx = context.WithValue(this.Context(), "testing", this.Name())
}

func (this *AdmissionFixture) Handle(context.Context, ...any) {}
func (this *AdmissionFixture) admit() bool {
	return this.allow
}
func (this *AdmissionFixture) await(ctx context.Context, message any) {
	this.So(ctx.Value("testing"), should.Equal, this.Name())
	this.messages = append(this.messages, message)
}

func (this *AdmissionFixture) ServeHTTP(http.ResponseWriter, *http.Request) {}

func (this *AdmissionFixture) TestAsHTTPHandler_ForwardsSingleMessageToAwait() {
	newHTTPAdapter(this).Handle(this.ctx, "x")
	this.So(this.messages, should.Equal, []any{"x"})
}

func (this *AdmissionFixture) TestAsHTTPHandler_ForwardsEachMessageInOrder() {
	newHTTPAdapter(this).Handle(this.ctx, "a", "b")
	this.So(this.messages, should.Equal, []any{"a", "b"})
}

func (this *AdmissionFixture) TestAdmission_PassesThroughWhenAdmitted() {
	var ran bool
	inner := http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		ran = true
		response.WriteHeader(http.StatusTeapot)
		_, _ = response.Write([]byte("inner"))
	})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/admin/orders", nil)
	this.allow = true
	newHTTPAdapter(this).HTTPHandler(inner).ServeHTTP(recorder, request)

	this.So(ran, should.BeTrue)
	this.So(recorder.Code, should.Equal, http.StatusTeapot)
	this.So(recorder.Body.String(), should.Equal, "inner")
}

func (this *AdmissionFixture) TestAdmission_Writes503WhenRejected() {
	var ran bool
	inner := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { ran = true })

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/admin/orders", nil)
	newHTTPAdapter(this).HTTPHandler(inner).ServeHTTP(recorder, request)

	this.So(ran, should.BeFalse)
	this.So(recorder.Code, should.Equal, http.StatusServiceUnavailable)
	this.So(recorder.Header().Get("Content-Type"), should.Equal, "application/json; charset=utf-8")
	this.So(recorder.Header().Get("Retry-After"), should.Equal, "1")
	this.So(recorder.Body.String(), should.Equal, `{"errors":[{"message":"service overloaded"}]}`)
}
