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

// @Summary Get Version
// @Description Get Version
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":"v1.3.1"}"
// @Router /api/v1/version [get]
func (controller *ConfigController) GetVersion(c *gin.Context) {
	version := strings.Split(config.GlobalConfig.Version, "-")[0]
	controller.wrapfunc(c, model.E_SUCCESS, version)
}
