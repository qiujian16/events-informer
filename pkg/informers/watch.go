package informers

import (
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/apis"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

type eventWatcher struct {
	uid    types.UID
	gvr    schema.GroupVersionResource
	stop   func()
	result chan watch.Event
	done   bool
}

func newEventWatcher(uid types.UID, stop func(), gvr schema.GroupVersionResource, chanSize int) *eventWatcher {
	return &eventWatcher{
		uid:    uid,
		gvr:    gvr,
		result: make(chan watch.Event, chanSize),
		stop:   stop,
		done:   false,
	}
}

func (w *eventWatcher) ResultChan() <-chan watch.Event {
	return w.result
}

func (w *eventWatcher) Stop() {
	w.done = true
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

	w.result <- *watchEvent
}

func (w *eventWatcher) process(ctx context.Context, receiver cloudevents.Client) {
	for {
		err := receiver.StartReceiver(ctx, func(event cloudevents.Event) error {
			klog.Info("receive wath response %v", event)
			if w.uid != types.UID(event.ID()) {
				return fmt.Errorf("unmatched event id")
			}

			if event.Type() != apis.EventWatchResponseType(w.gvr) {
				return fmt.Errorf("unmatched gvr got %s", event.Type())
			}

			response := &apis.WatchResponseEvent{}
			err := json.Unmarshal(event.Data(), response)
			if err != nil {
				return err
			}

			w.sendWatchCacheEvent(response)
			return nil
		})

		if err != nil {
			klog.Errorf("failed to receive watch event: %v", err)
		}

		if w.done {
			break
		}
	}
}
