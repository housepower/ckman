package nacos

import (
	"path/filepath"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/pkg/errors"
)

type NacosClient struct {
	Enabled     bool
	ServiceName string
	GroupName   string
	DataId      string
	Naming      naming_client.INamingClient
	Config      config_client.IConfigClient
}

func InitNacosClient(config *config.CKManNacosConfig, log string) (*NacosClient, error) {
	if config == nil {
		return nil, errors.Errorf("nacos config is invalid")
	}

	logDir, err := filepath.Abs(filepath.Dir(log))
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	if config.Enabled {
		clientConfig := constant.ClientConfig{
			NamespaceId:         config.NamespaceId,
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
		namingClient, err := clients.NewNamingClient(
			vo.NacosClientParam{
				ClientConfig:  &clientConfig,
				ServerConfigs: serverConfigs,
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		// Create config client for dynamic configuration
		configClient, err := clients.NewConfigClient(
			vo.NacosClientParam{
				ClientConfig:  &clientConfig,
				ServerConfigs: serverConfigs,
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		return &NacosClient{
			Enabled:     true,
			ServiceName: config.DataID,
			GroupName:   config.Group,
			DataId:      config.DataID,
			Naming:      namingClient,
			Config:      configClient,
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
		return false, errors.Errorf("naming client is nil")
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
		return false, errors.Errorf("naming client is nil")
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
		return nil, errors.Errorf("naming client is nil")
	}
}

func (c *NacosClient) Start(ipHttp string, portHttp int) error {
	if !c.Enabled {
		return nil
	}

	err := c.Subscribe()
	if err != nil {
		return errors.Wrap(err, "")
	}

	err = c.ListenConfig()
	if err != nil {
		return errors.Wrap(err, "")
	}

	var metadata map[string]string
	_, err = c.RegisterInstance(ipHttp, portHttp, metadata)
	if err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (c *NacosClient) Stop(ip string, port int) error {
	if !c.Enabled {
		return nil
	}

	_, err := c.DeregisterInstance(ip, port)
	if err != nil {
		return errors.Wrap(err, "")
	}

	err = c.Unsubscribe()
	if err != nil {
		return errors.Wrap(err, "")
	}

	if c.Config != nil {
		err = c.Config.CancelListenConfig(vo.ConfigParam{
			DataId: c.DataId,
			Group:  c.GroupName,
		})
		if err != nil {
			return errors.Wrap(err, "")
		}
	}

	c.Naming = nil
	c.Config = nil
	return nil
}

func (c *NacosClient) PublishConfig(content string) error {
	if c.Config != nil {
		_, err := c.Config.PublishConfig(vo.ConfigParam{
			DataId:  c.DataId,
			Group:   c.GroupName,
			Content: content,
		})
		if err != nil {
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func (c *NacosClient) GetConfig() (string, error) {
	if c.Config != nil {
		content, err := c.Config.GetConfig(vo.ConfigParam{
			DataId: c.DataId,
			Group:  c.GroupName})
		if err != nil && err.Error() != "config not found" {
			return "", err
		} else {
			return content, nil
		}
	}

	return "", nil
}

func (c *NacosClient) ListenConfig() error {
	if c.Config != nil {
		err := c.Config.ListenConfig(vo.ConfigParam{
			DataId:   c.DataId,
			Group:    c.GroupName,
			OnChange: ListenConfigCallback,
		})
		return errors.Wrap(err, "")
	}

	return nil
}

func ListenConfigCallback(namespace, group, dataId, data string) {
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
		return errors.Errorf("naming client is nil")
	}
}

func (c *NacosClient) SubscribeCallback(services []model.SubscribeService, err error) {
	log.Logger.Infof("service %s group %s changed", c.ServiceName, c.GroupName)
	instances, err1 := c.GetAllInstances()
	if err1 != nil {
		log.Logger.Errorf("get instances fail: %v", err1)
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
		return errors.Errorf("naming client is nil")
	}
}
