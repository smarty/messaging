package streaming

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/smarty/messaging/v3"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestManagerFixture(t *testing.T) {
	gunit.Run(new(ManagerFixture), t)
}

type ManagerFixture struct {
	*gunit.Fixture

	manager       messaging.ListenCloser
	subscriptions []Subscription

	mutex sync.Mutex

	subscriberCount        int
	subscriberContext      context.Context
	subscriberSubscription []Subscription

	closeCount  int
	listenCount int32

	listenSleep     time.Duration
	listenExitEarly bool
}

func (this *ManagerFixture) Setup() {
	const enoughSubscribersToExposeConcurrency = 64
	for i := 0; i < enoughSubscribersToExposeConcurrency; i++ {
		name := strconv.FormatInt(int64(i), 10)
		this.subscriptions = append(this.subscriptions, Subscription{name: name})
	}
	this.initializeManager()
}
func (this *ManagerFixture) initializeManager() {
	this.manager = newManager(this, this.subscriptions, this.newSubscriber)
}
func (this *ManagerFixture) newSubscriber(ctx context.Context, subscription Subscription) messaging.Listener {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.subscriberCount++
	this.subscriberContext = ctx
	this.subscriberSubscription = append(this.subscriberSubscription, subscription)
	return this
}

func (this *ManagerFixture) SkipTestWhenListening_NewSubscriberListenersCreatedAndStarted() {
	this.listenSleep = time.Millisecond * 5
	started := time.Now().UTC()

	go func() {
		time.Sleep(this.listenSleep * 3)
		closeResource(this.manager)
	}()
	this.manager.Listen()

	this.So(time.Since(started), should.BeBetween, this.listenSleep, this.listenSleep*10)
	this.So(this.subscriberCount, should.Equal, len(this.subscriptions))
	this.So(this.listenCount, should.Equal, len(this.subscriptions))
	this.So(this.subscriberContext, should.NotBeNil)
	this.So(this.subscriberContext, should.NotEqual, context.Background()) // derivative of it, but not background
}

func (this *ManagerFixture) TestWhenSubscriberListeningExitsEarly_AdditionalNewSubscriberListenersCreatedAndStarted() {
	this.listenSleep = 0
	this.listenExitEarly = true

	go func() {
		time.Sleep(time.Millisecond * 5)
		closeResource(this.manager)
	}()
	this.manager.Listen()

	this.So(this.subscriberCount, should.BeGreaterThan, len(this.subscriptions))
	this.So(this.listenCount, should.BeGreaterThan, len(this.subscriptions))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (this *ManagerFixture) Listen() {
	this.mutex.Lock()
	this.listenCount++
	ctx := this.subscriberContext
	this.mutex.Unlock()

	if this.listenExitEarly {
		return
	}

	time.Sleep(this.listenSleep)
	<-ctx.Done()
}
func (this *ManagerFixture) Close() error {
	this.closeCount++
	return nil
}
