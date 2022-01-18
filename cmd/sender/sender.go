package main

import (
	"context"
	"flag"

	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/qiujian16/events-informer/pkg/senders"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

func main() {
	var kubeConfig string
	var kafkaEndpoint string

	flag.StringVar(&kubeConfig, "kubeconfig", "",
		"Paths to a kubeconfig connect to hub.")
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

	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		klog.Fatalf("failed to build config, %v", err)
	}

	dynamicClient := dynamic.NewForConfigOrDie(restConfig)

	s := senders.NewDynamicSender(dynamicClient)

	transport := senders.NewDefaultSenderTansport(s, c)

	transport.Run(context.Background())
}
