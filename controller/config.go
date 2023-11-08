package controller

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
)

type ConfigController struct {
	Controller
	signal chan os.Signal
}

func NewConfigController(ch chan os.Signal, wrapfunc Wrapfunc) *ConfigController {
	cf := &ConfigController{}
	cf.signal = ch
	cf.wrapfunc = wrapfunc
	return cf
}

// 该接口不暴露给用户
func (controller *ConfigController) GetVersion(c *gin.Context) {
	version := strings.Split(config.GlobalConfig.Version, "-")[0]
	controller.wrapfunc(c, model.E_SUCCESS, version)
}
