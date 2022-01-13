package senders

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

type Sender interface {
	List(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (*unstructured.UnstructuredList, error)
	Watch(namespace string, gvr schema.GroupVersionResource, options metav1.ListOptions) (watch.Interface, error)
}

type SenderTransport interface {
	Run(ctx context.Context)
}
