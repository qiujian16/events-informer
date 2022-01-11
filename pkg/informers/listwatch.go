package informers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
)

type EventListWatcher struct {
	eventClient    cloudevents.Client
	gvr            schema.GroupVersionResource
	source         string
	ctx            context.Context
	listResultChan map[types.UID]chan ListResponseEvent
	rwlock         sync.RWMutex
}

type Event interface {
	ToCloudEvent() cloudevents.Event
}

type ListWatchEvent struct {
	uid     types.UID
	gvr     schema.GroupVersionResource
	options metav1.ListOptions
	mode    string
	source  string
}

type ListResponseEvent struct {
	Objects   *unstructured.UnstructuredList `json:"objects"`
	EndOfList bool                           `json:"endOfList"`
}

func newListEvent(source string, gvr schema.GroupVersionResource, options metav1.ListOptions) *ListWatchEvent {
	return &ListWatchEvent{
		uid:     uuid.NewUUID(),
		gvr:     gvr,
		options: options,
		mode:    "list",
	}
}

func newWatchEvent(source string, gvr schema.GroupVersionResource, options metav1.ListOptions) *ListWatchEvent {
	return &ListWatchEvent{
		uid:     uuid.NewUUID(),
		gvr:     gvr,
		options: options,
		mode:    "list",
	}
}

func (l *ListWatchEvent) ToCloudEvent() cloudevents.Event {
	evt := cloudevents.NewEvent()
	evt.SetID(string(l.uid))
	evt.SetType(fmt.Sprintf("%s.%s", l.mode, toGVRString(l.gvr)))
	evt.SetSource(l.source)
	evt.SetData(cloudevents.ApplicationJSON, l.options)
	return evt
}

func NewEventListWatcher(ctx context.Context, source string, client cloudevents.Client, gvr schema.GroupVersionResource) *EventListWatcher {
	lw := &EventListWatcher{
		source:         source,
		eventClient:    client,
		gvr:            gvr,
		ctx:            ctx,
		listResultChan: map[types.UID]chan ListResponseEvent{},
	}

	// start list receiver
	go client.StartReceiver(ctx, func(evt cloudevents.Event) error {
		lw.rwlock.RLock()
		defer lw.rwlock.RUnlock()

		resulctChan, ok := lw.listResultChan[types.UID(evt.ID())]

		if !ok {
			return fmt.Errorf("unable to find the related uid for list %s", evt.ID())
		}

		if evt.Type() != fmt.Sprintf("response.list.%s", toGVRString(lw.gvr)) {
			return nil
		}

		response := &ListResponseEvent{}
		err := json.Unmarshal(evt.Data(), response)
		if err != nil {
			return err
		}

		select {
		case resulctChan <- *response:
		case <-ctx.Done():
		}

		return nil
	})

	return lw
}

func (e *EventListWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return e.list(e.ctx, options)
}

func (e *EventListWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return e.watch(e.ctx, options)
}

func (e *EventListWatcher) watch(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	watchEvent := newWatchEvent(e.source, e.gvr, options)

	result := e.eventClient.Send(ctx, watchEvent.ToCloudEvent())
	if cloudevents.IsUndelivered(result) {
		return nil, fmt.Errorf("failed to send list event, %v", result)
	}

	watcher := newEventWatcher(watchEvent.uid, e.gvr, 10)

	go e.eventClient.StartReceiver(ctx, watcher.process)
	return watcher, nil
}

func (e *EventListWatcher) list(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	listEvent := newListEvent(e.source, e.gvr, options)

	result := e.eventClient.Send(ctx, listEvent.ToCloudEvent())
	if cloudevents.IsUndelivered(result) {
		return nil, fmt.Errorf("failed to send list event, %v", result)
	}

	objectList := &unstructured.UnstructuredList{}

	// now start to recieve the list response until endofList is false
	e.listResultChan[listEvent.uid] = make(chan ListResponseEvent)
	defer delete(e.listResultChan, listEvent.uid)
	for {
		select {
		case response, ok := <-e.listResultChan[listEvent.uid]:
			if !ok {
				return objectList, nil
			}

			if objectList.Object == nil {
				objectList.Object = response.Objects.Object
			}

			objectList.Items = append(objectList.Items, response.Objects.Items...)
			if response.EndOfList {
				return objectList, nil
			}
		case <-ctx.Done():
			return objectList, nil
		}
	}
}

func toGVRString(gvr schema.GroupVersionResource) string {
	return fmt.Sprintf("%s.%s.%s", gvr.Version, gvr.Resource, gvr.Group)
}
