package lb

import (
	"context"
	"go.etcd.io/etcd/clientv3"
	"time"
)

type ServiceRegister struct {
	client *clientv3.Client
	leaseID clientv3.LeaseID
	key string
	val string
	keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
}

func NewServiceRegister(endpoints []string, key, val string) (*ServiceRegister, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
		DialTimeout: 5*time.Second,
	})
	if err != nil {
		return nil, err
	}
	service := &ServiceRegister{
		client:  client,
		leaseID: 0,
		key:     key,
		val:     val,
	}
	return service, nil
}

func (s *ServiceRegister) Run(ctx context.Context, lease int64) (<-chan *clientv3.LeaseKeepAliveResponse, error){
	gresp, err := s.client.Grant(ctx, lease)
	if err != nil {
		return nil, err
	}
	_, err = s.client.Put(ctx, s.key, s.val, clientv3.WithLease(gresp.ID))
	if err != nil {
		return nil, err
	}
	// 开启心跳刷新
	keepAliveChan, err := s.client.KeepAlive(ctx, gresp.ID)
	if err != nil {
		return nil, err
	}
	s.leaseID = gresp.ID
	s.keepAliveChan = keepAliveChan
	return s.keepAliveChan, err
}

func (s *ServiceRegister) Stop(ctx context.Context) {
	_, _ = s.client.Revoke(ctx, s.leaseID)
	_ = s.client.Close()
}