package senders

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type dynamicSender struct {
	client dynamic.Interface
}

func NewDynamicSender(client dynamic.Interface) Sender {
	return &dynamicSender{
		client: client,
	}
}

func (d *dynamicSender) List(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return d.client.Resource(gvr).Namespace(namespace).List(context.TODO(), options)
}

func (d *dynamicSender) Watch(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (watch.Interface, error) {
	return d.client.Resource(gvr).Namespace(namespace).Watch(context.TODO(), options)
}
