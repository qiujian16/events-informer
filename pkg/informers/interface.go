package informers

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
)

type EventSharedInformerFactory interface {
	Start()
	ForResource(gvr schema.GroupVersionResource) informers.GenericInformer
	WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool
}
