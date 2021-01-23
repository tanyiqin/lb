package lb

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"sync"
	"time"
)

type ServiceDiscovery struct {
	client *clientv3.Client
	serviceList map[string]string
	mutex sync.RWMutex
}

func NewServiceDiscovery(endpoints []string) (*ServiceDiscovery, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	ser := &ServiceDiscovery{
		client: client,
		serviceList: make(map[string]string),
	}
	return ser, nil
}

func (s *ServiceDiscovery) RefreshServiceList(ctx context.Context, prefix string) error{
	gresp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range gresp.Kvs {
		s.Set(string(kv.Key), string(kv.Value))
	}
	return nil
}

func (s *ServiceDiscovery) Run(ctx context.Context, prefix string) <-chan clientv3.WatchResponse{
	watchChan := s.client.Watch(ctx, prefix, clientv3.WithPrefix())
	_ = s.RefreshServiceList(ctx, prefix)
	return watchChan
}

func (s *ServiceDiscovery) Set(key, val string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.serviceList[key] = val
}

func (s *ServiceDiscovery) Get(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.serviceList[key]
}

func (s *ServiceDiscovery) Del(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.serviceList, key)
}