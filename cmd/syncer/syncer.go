package main

import (
	"context"
	"flag"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/informers"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

func main() {
	ctx := context.TODO()
	var kafkaEndpoint string

	flag.StringVar(&kafkaEndpoint, "kafka-endpoint", "",
		"Kafka endpoint.")
	flag.Parse()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_0_0_0

	sender, err := kafka_sarama.NewSender([]string{kafkaEndpoint}, saramaConfig, "request-topic")
	if err != nil {
		klog.Fatalf("failed to create protocol: %s", err.Error())
	}
	defer sender.Close(ctx)

	receiver, err := kafka_sarama.NewConsumer([]string{kafkaEndpoint}, saramaConfig, "response-group-id", "response-topic")
	if err != nil {
		klog.Fatalf("failed to create protocol: %s", err.Error())
	}
	defer receiver.Close(ctx)

	s, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		klog.Fatalf("failed to create client, %v", err)
	}

	r, err := cloudevents.NewClient(receiver)
	if err != nil {
		klog.Fatalf("failed to create client, %v", err)
	}

	informerFactory := informers.NewEventsSharedInformerFactory(ctx, s, r, 5*time.Minute)

	informer := informerFactory.ForResource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"})

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			accessor, _ := meta.Accessor(obj)
			klog.Infof("added %s/%s", accessor.GetName(), accessor.GetNamespace())
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldAccessor, _ := meta.Accessor(oldObj)
			newAccessor, _ := meta.Accessor(newObj)
			klog.Infof("Updated from %s/%s to %s/%s", oldAccessor.GetNamespace(), oldAccessor.GetName(), newAccessor.GetNamespace(), newAccessor.GetName())
		},
		DeleteFunc: func(obj interface{}) {
			klog.Infof("deleted %v", obj)
		},
	})

	informerFactory.Start()
	<-ctx.Done()
}
