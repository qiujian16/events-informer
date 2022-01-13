package senders

import (
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/apis"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

type defaultSenderTansport struct {
	sender      Sender
	eventClient cloudevents.Client
	watchStop   map[types.UID]context.CancelFunc
}

func NewDefaultSenderTansport(sender Sender, evtClient cloudevents.Client) SenderTransport {
	return &defaultSenderTansport{
		sender:      sender,
		eventClient: evtClient,
		watchStop:   map[types.UID]context.CancelFunc{},
	}
}

func (d *defaultSenderTansport) Run(ctx context.Context) {
	d.eventClient.StartReceiver(ctx, func(evt cloudevents.Event) error {
		mode, gvr, err := apis.ParseEventType(evt.Type())
		if err != nil {
			return err
		}

		req := &apis.RequestEvent{}
		err = json.Unmarshal(evt.Data(), &req)
		if err != nil {
			return err
		}

		switch mode {
		case "list":
			return d.sendListResponses(ctx, types.UID(evt.ID()), req.Namespace, gvr, req.Options)
		case "watch":
			go d.watchResponse(ctx, types.UID(evt.ID()), req.Namespace, gvr, req.Options)
		case "stopwatch":
			cancelFunc, ok := d.watchStop[types.UID(evt.ID())]
			if ok {
				cancelFunc()
				delete(d.watchStop, types.UID(evt.ID()))
			}
		}
		return nil
	})
}

func (d *defaultSenderTansport) watchResponse(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) error {
	w, err := d.sender.Watch(namespace, gvr, options)
	if err != nil {
		return err
	}

	watchCtx, stop := context.WithCancel(ctx)
	d.watchStop[id] = stop
	defer w.Stop()

	for {
		select {
		case e, ok := <-w.ResultChan():
			if !ok {
				return fmt.Errorf("failed to watch the result")
			}

			response := &apis.WatchResponseEvent{
				Type:   e.Type,
				Object: e.Object.(*unstructured.Unstructured),
			}

			evt := cloudevents.NewEvent()
			evt.SetID(string(id))
			evt.SetType(apis.EventWatchResponseType(gvr))
			evt.SetData(cloudevents.ApplicationJSON, response)

			klog.Infof("send watch response for resource %v", gvr)
			result := d.eventClient.Send(ctx, evt)

			if len(result.Error()) > 0 {
				klog.Errorf(result.Error())
			}
		case <-watchCtx.Done():
			return nil
		}
	}
}

func (d *defaultSenderTansport) sendListResponses(ctx context.Context, id types.UID, namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) error {
	objs, err := d.sender.List(namespace, gvr, options)
	if err != nil {
		return err
	}

	response := &apis.ListResponseEvent{
		Objects:   objs,
		EndOfList: true,
	}

	evt := cloudevents.NewEvent()
	evt.SetID(string(id))
	evt.SetType(apis.EventListResponseType(gvr))
	evt.SetData(cloudevents.ApplicationJSON, response)

	klog.Infof("send list response for resource %v", gvr)
	result := d.eventClient.Send(ctx, evt)

	if len(result.Error()) > 0 {
		return fmt.Errorf(result.Error())
	}

	return nil
}
