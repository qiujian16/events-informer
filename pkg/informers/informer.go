package informers

import (
	"context"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/dynamic/dynamiclister"
	"k8s.io/client-go/informers"
	cache "k8s.io/client-go/tools/cache"
)

type eventInformer struct {
	informer cache.SharedIndexInformer
	gvr      schema.GroupVersionResource
}

type eventSharedInformerFactory struct {
	ctx           context.Context
	client        cloudevents.Client
	defaultResync time.Duration
	namespace     string

	lock      sync.Mutex
	informers map[schema.GroupVersionResource]informers.GenericInformer
	// startedInformers is used for tracking which informers have been started.
	// This allows Start() to be called multiple times safely.
	startedInformers map[schema.GroupVersionResource]bool
	tweakListOptions dynamicinformer.TweakListOptionsFunc
}

func NewEventsSharedInformerFactory(ctx context.Context, client cloudevents.Client, defaultResync time.Duration) EventSharedInformerFactory {
	return NewFilteredEventSharedInformerFactory(ctx, client, defaultResync, metav1.NamespaceAll, nil)
}

// NewFilteredDynamicSharedInformerFactory constructs a new instance of dynamicSharedInformerFactory.
// Listers obtained via this factory will be subject to the same filters as specified here.
func NewFilteredEventSharedInformerFactory(ctx context.Context, client cloudevents.Client, defaultResync time.Duration, namespace string, tweakListOptions dynamicinformer.TweakListOptionsFunc) EventSharedInformerFactory {
	return &eventSharedInformerFactory{
		ctx:              ctx,
		client:           client,
		defaultResync:    defaultResync,
		namespace:        namespace,
		informers:        map[schema.GroupVersionResource]informers.GenericInformer{},
		startedInformers: make(map[schema.GroupVersionResource]bool),
		tweakListOptions: tweakListOptions,
	}
}

var _ EventSharedInformerFactory = &eventSharedInformerFactory{}

func (f *eventSharedInformerFactory) ForResource(gvr schema.GroupVersionResource) informers.GenericInformer {
	f.lock.Lock()
	defer f.lock.Unlock()

	key := gvr
	informer, exists := f.informers[key]
	if exists {
		return informer
	}

	informer = NewFilteredEventsInformer(f.ctx, f.client, gvr, f.namespace, f.defaultResync, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
	f.informers[key] = informer

	return informer
}

// Start initializes all requested informers.
func (f *eventSharedInformerFactory) Start() {
	f.lock.Lock()
	defer f.lock.Unlock()

	for informerType, informer := range f.informers {
		if !f.startedInformers[informerType] {
			go informer.Informer().Run(f.ctx.Done())
			f.startedInformers[informerType] = true
		}
	}
}

// WaitForCacheSync waits for all started informers' cache were synced.
func (f *eventSharedInformerFactory) WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool {
	informers := func() map[schema.GroupVersionResource]cache.SharedIndexInformer {
		f.lock.Lock()
		defer f.lock.Unlock()

		informers := map[schema.GroupVersionResource]cache.SharedIndexInformer{}
		for informerType, informer := range f.informers {
			if f.startedInformers[informerType] {
				informers[informerType] = informer.Informer()
			}
		}
		return informers
	}()

	res := map[schema.GroupVersionResource]bool{}
	for informType, informer := range informers {
		res[informType] = cache.WaitForCacheSync(stopCh, informer.HasSynced)
	}
	return res
}

func NewFilteredEventsInformer(
	ctx context.Context,
	eventClient cloudevents.Client,
	gvr schema.GroupVersionResource,
	namespace string,
	resyncPeriod time.Duration,
	indexers cache.Indexers,
	tweakListOptions dynamicinformer.TweakListOptionsFunc) informers.GenericInformer {
	lw := NewEventListWatcher(ctx, "agent", namespace, eventClient, gvr)

	return &eventInformer{
		gvr: gvr,
		informer: cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return lw.List(options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return lw.Watch(options)
				},
			},
			&unstructured.Unstructured{},
			resyncPeriod,
			indexers,
		),
	}
}

func (e *eventInformer) Informer() cache.SharedIndexInformer {
	return e.informer
}

func (e *eventInformer) Lister() cache.GenericLister {
	return dynamiclister.NewRuntimeObjectShim(dynamiclister.New(e.informer.GetIndexer(), e.gvr))
}
