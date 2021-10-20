package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"os"
	"strings"
)

type ConfigController struct {
	signal chan os.Signal
}

func NewConfigController(ch chan os.Signal) *ConfigController {
	cf := &ConfigController{}
	cf.signal = ch
	return cf
}

// @Summary Get Version
// @Description Get Version
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":"v1.3.1"}"
// @Router /api/v1/version [get]
func (cf ConfigController) GetVersion(c *gin.Context) {
	version := strings.Split(config.GlobalConfig.Version, "-")[0]
	model.WrapMsg(c, model.SUCCESS, version)
}