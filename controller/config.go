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
	// Nacos 订阅回调存在 self 不出现的情况：ckman 先 Subscribe 再 RegisterInstance，
	// 首次 push 时本机尚未注册；其后 Nacos 不再向本机的订阅 push 自己注册的实例。
	// 这里始终把 self union 进去并去重，保证下拉里能看到自己。
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
