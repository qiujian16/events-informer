package main

import (
	"context"
	"flag"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/informers"
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

	sender, err := kafka_sarama.NewSender([]string{"127.0.0.1:9092"}, saramaConfig, "test-topic")
	if err != nil {
		klog.Fatalf("failed to create protocol: %s", err.Error())
	}

	defer sender.Close(context.Background())

	c, err := cloudevents.NewClient(sender, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		klog.Fatalf("failed to create client, %v", err)
	}

	informerFactory := informers.NewEventsSharedInformerFactory(ctx, c, 5*time.Minute)

	informer := informerFactory.ForResource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"})

	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			klog.Infof("added %v", obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			klog.Infof("Updated from %v to %v", oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			klog.Infof("deleted %v", obj)
		},
	})

	informerFactory.Start()
	<-ctx.Done()
}
