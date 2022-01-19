package informers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/apis"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"
)

type EventListWatcher struct {
	sender         cloudevents.Client
	receiver       cloudevents.Client
	gvr            schema.GroupVersionResource
	source         string
	namespace      string
	ctx            context.Context
	listResultChan map[types.UID]chan apis.ListResponseEvent
	rwlock         sync.RWMutex
}

type Event interface {
	ToCloudEvent() cloudevents.Event
}

type ListWatchEvent struct {
	uid       types.UID
	gvr       schema.GroupVersionResource
	options   metav1.ListOptions
	mode      string
	source    string
	namespace string
}

func newListWatchEvent(source, mode, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) *ListWatchEvent {
	return &ListWatchEvent{
		uid:       uuid.NewUUID(),
		gvr:       gvr,
		options:   options,
		mode:      mode,
		namespace: namespace,
		source:    source,
	}
}

func (l *ListWatchEvent) ToCloudEvent() cloudevents.Event {
	evt := cloudevents.NewEvent()

	data := &apis.RequestEvent{
		Namespace: l.namespace,
		Options:   l.options,
	}

	evt.SetType(l.mode)
	evt.SetID(string(l.uid))
	evt.SetSource(l.source)
	evt.SetData(cloudevents.ApplicationJSON, data)
	return evt
}

func NewEventListWatcher(ctx context.Context, source, namespace string, sender, receiver cloudevents.Client, gvr schema.GroupVersionResource) *EventListWatcher {
	lw := &EventListWatcher{
		source:         source,
		sender:         sender,
		receiver:       receiver,
		gvr:            gvr,
		ctx:            ctx,
		namespace:      namespace,
		listResultChan: map[types.UID]chan apis.ListResponseEvent{},
	}

	// start list receiver
	go receiver.StartReceiver(ctx, func(evt cloudevents.Event) error {
		lw.rwlock.RLock()
		defer lw.rwlock.RUnlock()

		resulctChan, ok := lw.listResultChan[types.UID(evt.ID())]

		if !ok {
			return fmt.Errorf("unable to find the related uid for list %s", evt.ID())
		}

		if evt.Type() != apis.EventListResponseType(lw.gvr) {
			return nil
		}

		response := &apis.ListResponseEvent{}
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
	watchEvent := newListWatchEvent(e.source, apis.EventWatchType(e.gvr), e.namespace, e.gvr, options)

	result := e.sender.Send(ctx, watchEvent.ToCloudEvent())
	if cloudevents.IsUndelivered(result) {
		return nil, fmt.Errorf("failed to send list event, %v", result)
	}

	klog.Infof("sent watch event with result %v", result)

	watcher := newEventWatcher(watchEvent.uid, e.stopWatch, e.gvr, 10)

	go watcher.process(ctx, e.receiver)
	return watcher, nil
}

func (e *EventListWatcher) stopWatch() {
	stopWatch := newListWatchEvent(e.source, apis.EventStopWatchType(e.gvr), e.namespace, e.gvr, metav1.ListOptions{})
	result := e.sender.Send(e.ctx, stopWatch.ToCloudEvent())

	if cloudevents.IsUndelivered(result) {
		utilruntime.HandleError(fmt.Errorf(result.Error()))
	}
}

func (e *EventListWatcher) list(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	listEvent := newListWatchEvent(e.source, apis.EventListType(e.gvr), e.namespace, e.gvr, options)

	result := e.sender.Send(ctx, listEvent.ToCloudEvent())
	if cloudevents.IsUndelivered(result) {
		return nil, fmt.Errorf("failed to send list event, %v", result)
	}

	klog.Infof("sent list event with result %v", result)

	objectList := &unstructured.UnstructuredList{}

	// now start to recieve the list response until endofList is false
	e.listResultChan[listEvent.uid] = make(chan apis.ListResponseEvent)
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
