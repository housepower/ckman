package nacos

import (
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"path/filepath"
)

var NacosServiceName = "ckman"
var NacosDefaultGroupName = "DEFAULT_GROUP"

type NacosClient struct {
	Enabled     bool
	ServiceName string
	GroupName   string
	Naming      naming_client.INamingClient
	Config      config_client.IConfigClient
}

func InitNacosClient(config *config.CKManNacosConfig, log string, group string) (*NacosClient, error) {
	if config == nil {
		return nil, fmt.Errorf("nacos config is invalid")
	}

	logDir, err := filepath.Abs(filepath.Dir(log))
	if err != nil {
		return nil, err
	}

	if config.Enabled {
		clientConfig := constant.ClientConfig{
			NamespaceId:         config.Namespace,
			TimeoutMs:           5000,
			NotLoadCacheAtStart: true,
			LogDir:              logDir,
			CacheDir:            logDir,
			RotateTime:          "24h",
			MaxAge:              3,
			LogLevel:            "info",
			Username:            config.UserName,
			Password:            config.Password,
		}

		var serverConfigs []constant.ServerConfig
		// At least one ServerConfig
		for _, host := range config.Hosts {
			server := constant.ServerConfig{
				IpAddr:      host,
				ContextPath: "/nacos",
				Port:        config.Port,
			}
			serverConfigs = append(serverConfigs, server)
		}

		// Create naming client for service discovery
		namingClient, err := clients.CreateNamingClient(map[string]interface{}{
			"serverConfigs": serverConfigs,
			"clientConfig":  clientConfig,
		})
		if err != nil {
			return nil, err
		}

		//Create config client for dynamic configuration
		//configClient, err := clients.CreateConfigClient(map[string]interface{}{
		//	"serverConfigs": serverConfigs,
		//	"clientConfig":  clientConfig,
		//}

		return &NacosClient{
			Enabled:     true,
			ServiceName: NacosServiceName,
			GroupName:   group,
			Naming:      namingClient,
			Config:      nil,
		}, nil
	}

	return &NacosClient{
		Enabled: false,
	}, nil
}

func (c *NacosClient) RegisterInstance(ip string, port int, metadata map[string]string) (bool, error) {
	if c.Naming != nil {
		return c.Naming.RegisterInstance(vo.RegisterInstanceParam{
			Ip:          ip,
			Port:        uint64(port),
			ServiceName: c.ServiceName,
			Weight:      10,
			Enable:      true,
			Healthy:     true,
			Ephemeral:   true,
			Metadata:    metadata,
			GroupName:   c.GroupName, // default value is DEFAULT_GROUP
		})
	} else {
		return false, fmt.Errorf("naming client is nil")
	}
}

func (c *NacosClient) DeregisterInstance(ip string, port int) (bool, error) {
	if c.Naming != nil {
		return c.Naming.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          ip,
			Port:        uint64(port),
			ServiceName: c.ServiceName,
			Ephemeral:   false,
			GroupName:   c.GroupName, // default value is DEFAULT_GROUP
		})
	} else {
		return false, fmt.Errorf("naming client is nil")
	}
}

func (c *NacosClient) GetAllInstances() ([]model.Instance, error) {
	if c.Naming != nil {
		// SelectInstances only return the instances of healthy=${HealthyOnly},enable=true and weight>0
		return c.Naming.SelectInstances(vo.SelectInstancesParam{
			ServiceName: c.ServiceName,
			GroupName:   c.GroupName, // default value is DEFAULT_GROUP
			HealthyOnly: true,
		})
	} else {
		return nil, fmt.Errorf("naming client is nil")
	}
}

func (c *NacosClient) Start(ipHttp string, portHttp int) error {
	if !c.Enabled {
		return nil
	}

	err := c.Subscribe()
	if err != nil {
		return err
	}

	_, err = c.RegisterInstance(ipHttp, portHttp, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *NacosClient) Stop(ip string, port int) error {
	if !c.Enabled {
		return nil
	}

	_, err := c.DeregisterInstance(ip, port)
	if err != nil {
		return err
	}

	err = c.Unsubscribe()
	if err != nil {
		return err
	}

	c.Naming = nil
	return nil
}

func (c *NacosClient) Subscribe() error {
	if c.Naming != nil {
		// SelectInstances only return the instances of healthy=${HealthyOnly},enable=true and weight>0
		return c.Naming.Subscribe(&vo.SubscribeParam{
			ServiceName:       c.ServiceName,
			GroupName:         c.GroupName, // default value is DEFAULT_GROUP
			SubscribeCallback: c.SubscribeCallback,
		})
	} else {
		return fmt.Errorf("naming client is nil")
	}
}

func (c *NacosClient) SubscribeCallback(services []model.SubscribeService, err error) {
	log.Logger.Infof("service %s group %s changed", c.ServiceName, c.GroupName)
	instances, err := c.GetAllInstances()
	if err != nil {
		log.Logger.Errorf("get instances fail: %v", err)
		return
	}

	// 获取集群里的所有cell节点
	config.ClusterMutex.Lock()
	defer config.ClusterMutex.Unlock()
	config.ClusterNodes = make([]config.ClusterNode, 0)
	for _, instance := range instances {
		node := config.ClusterNode{
			Ip:   instance.Ip,
			Port: int(instance.Port),
		}
		config.ClusterNodes = append(config.ClusterNodes, node)
	}

	log.Logger.Infof("nacos instances %v", config.ClusterNodes)
}

func (c *NacosClient) Unsubscribe() error {
	if c.Naming != nil {
		// SelectInstances only return the instances of healthy=${HealthyOnly},enable=true and weight>0
		return c.Naming.Unsubscribe(&vo.SubscribeParam{
			ServiceName:       c.ServiceName,
			GroupName:         c.GroupName, // default value is DEFAULT_GROUP
			SubscribeCallback: c.SubscribeCallback,
		})
	} else {
		return fmt.Errorf("naming client is nil")
	}
}
