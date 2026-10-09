package nacos

import (
	"errors"
	"fmt"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

type Registry struct {
	naming   naming_client.INamingClient
	service  string
	group    string
	self     config.ClusterNode
	onChange func([]config.ClusterNode)
}

func NewRegistry(c *config.CKManNacosConfig, service, cacheDir string) (*Registry, error) {
	naming, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  clientConfig(c, c.NamespaceId, cacheDir),
		ServerConfigs: serverConfigs(c),
	})
	if err != nil {
		return nil, fmt.Errorf("create nacos naming client: %w", err)
	}
	return &Registry{naming: naming, service: service, group: c.Group}, nil
}

func (r *Registry) OnChange(fn func([]config.ClusterNode)) { r.onChange = fn }

func (r *Registry) Start(self config.ClusterNode) error {
	r.self = self
	if err := r.naming.Subscribe(r.subscribeParam()); err != nil {
		return fmt.Errorf("nacos subscribe: %w", err)
	}
	if _, err := r.naming.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          self.Ip,
		Port:        uint64(self.Port),
		ServiceName: r.service,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		GroupName:   r.group, // default value is DEFAULT_GROUP
	}); err != nil {
		return fmt.Errorf("nacos register instance: %w", err)
	}
	return nil
}

func (r *Registry) Stop() error {
	var errs []error
	if _, err := r.naming.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          r.self.Ip,
		Port:        uint64(r.self.Port),
		ServiceName: r.service,
		Ephemeral:   true, // 与注册时一致
		GroupName:   r.group,
	}); err != nil {
		errs = append(errs, fmt.Errorf("nacos deregister instance: %w", err))
	}
	if err := r.naming.Unsubscribe(r.subscribeParam()); err != nil {
		errs = append(errs, fmt.Errorf("nacos unsubscribe: %w", err))
	}
	r.naming.CloseClient()
	return errors.Join(errs...)
}

func (r *Registry) subscribeParam() *vo.SubscribeParam {
	return &vo.SubscribeParam{
		ServiceName:       r.service,
		GroupName:         r.group,
		SubscribeCallback: r.subscribeCallback,
	}
}

func (r *Registry) subscribeCallback(_ []model.Instance, _ error) {
	log.Logger.Infof("service %s group %s changed", r.service, r.group)
	// SelectInstances only return the instances of healthy=${HealthyOnly},enable=true and weight>0
	instances, err := r.naming.SelectInstances(vo.SelectInstancesParam{
		ServiceName: r.service,
		GroupName:   r.group,
		HealthyOnly: true,
	})
	if err != nil {
		log.Logger.Errorf("get instances fail: %v", err)
		return
	}
	nodes := make([]config.ClusterNode, 0, len(instances))
	for _, instance := range instances {
		nodes = append(nodes, config.ClusterNode{Ip: instance.Ip, Port: int(instance.Port)})
	}
	log.Logger.Infof("nacos instances %v", nodes)
	if r.onChange != nil {
		r.onChange(nodes)
	}
}
