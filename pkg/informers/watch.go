package informers

import (
	"encoding/json"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/apis"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

type eventWatcher struct {
	uid    types.UID
	gvr    schema.GroupVersionResource
	stop   func()
	result chan watch.Event
	done   chan struct{}
}

func newEventWatcher(uid types.UID, stop func(), gvr schema.GroupVersionResource, chanSize int) *eventWatcher {
	return &eventWatcher{
		uid:    uid,
		gvr:    gvr,
		result: make(chan watch.Event, chanSize),
		done:   make(chan struct{}),
		stop:   stop,
	}
}

func (w *eventWatcher) ResultChan() <-chan watch.Event {
	return w.result
}

func (w *eventWatcher) Stop() {
	w.stop()
}

func (w *eventWatcher) convertToWatchEvent(event *apis.WatchResponseEvent) *watch.Event {
	return &watch.Event{
		Type:   event.Type,
		Object: event.Object,
	}
}

func (w *eventWatcher) sendWatchCacheEvent(event *apis.WatchResponseEvent) {
	watchEvent := w.convertToWatchEvent(event)
	if watchEvent == nil {
		// Watcher is not interested in that object.
		return
	}

	// We need to ensure that if we put event X to the c.result, all
	// previous events were already put into it before, no matter whether
	// c.done is close or not.
	// Thus we cannot simply select from c.done and c.result and this
	// would give us non-determinism.
	// At the same time, we don't want to block infinitely on putting
	// to c.result, when c.done is already closed.

	// This ensures that with c.done already close, we at most once go
	// into the next select after this. With that, no matter which
	// statement we choose there, we will deliver only consecutive
	// events.
	select {
	case <-w.done:
		return
	default:
	}

	select {
	case w.result <- *watchEvent:
	case <-w.done:
	}
}

func (w *eventWatcher) process(event cloudevents.Event) error {
	if w.uid != types.UID(event.ID()) {
		return nil
	}

	if event.Type() != apis.EventWatchResponseType(w.gvr) {
		return nil
	}

	response := &apis.WatchResponseEvent{}
	err := json.Unmarshal(event.Data(), response)
	if err != nil {
		return err
	}

	w.sendWatchCacheEvent(response)
	return nil
}
