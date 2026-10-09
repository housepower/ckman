package controller

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
)

type ConfigController struct {
	Controller
	config *config.CKManConfig
	signal chan os.Signal
}

func NewConfigController(ch chan os.Signal, config *config.CKManConfig, wrapfunc Wrapfunc) *ConfigController {
	cf := &ConfigController{}
	cf.signal = ch
	cf.config = config
	cf.wrapfunc = wrapfunc
	return cf
}

// 该接口不暴露给用户
func (controller *ConfigController) GetVersion(c *gin.Context) {
	version := strings.Split(config.GlobalConfig.Version, "-")[0]
	controller.wrapfunc(c, model.E_SUCCESS, version)
}

func (controller *ConfigController) GetInstances(c *gin.Context) {
	// config.SetClusterNodes 已并入 self；但注册中心在 HTTP server 之后才启动，
	// 那之前 ClusterNodes 为空，这里仍把 self union 进去，保证下拉里能看到自己。
	self := net.JoinHostPort(controller.config.Server.Ip, fmt.Sprint(controller.config.Server.Port))
	seen := map[string]struct{}{}
	var instances []string
	add := func(addr string) {
		if _, ok := seen[addr]; ok {
			return
		}
		seen[addr] = struct{}{}
		instances = append(instances, addr)
	}

	add(self)
	config.ClusterMutex.RLock()
	for _, instance := range config.ClusterNodes {
		add(net.JoinHostPort(instance.Ip, fmt.Sprint(instance.Port)))
	}
	config.ClusterMutex.RUnlock()

	controller.wrapfunc(c, model.E_SUCCESS, instances)
}
