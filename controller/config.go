package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"os"
	"strings"
	"syscall"
)

type ConfigController struct {
	signal chan os.Signal
}

func NewConfigController(ch chan os.Signal) *ConfigController {
	cf := &ConfigController{}
	cf.signal = ch
	return cf
}

// @Summary Update Config
// @Description Update Config
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.UpdateConfigReq true "request body"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5070","retMsg":"update config failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/v1/config [put]
func (cf *ConfigController) UpdateConfig(c *gin.Context) {
	var req model.UpdateConfigReq

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	config.GlobalConfig.Prometheus.Hosts = req.Prometheus

	if err := config.MarshConfigFile(); err != nil {
		model.WrapMsg(c, model.UPDATE_CONFIG_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
	cf.signal <- syscall.SIGHUP
}

// @Summary Get Config
// @Description Get Config
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":{"peers":null,"prometheus":["192.168.101.105:19090"],"alertManagers":null}}"
// @Router /api/v1/config [get]
func (cf *ConfigController) GetConfig(c *gin.Context) {
	var req model.UpdateConfigReq

	req.Prometheus = config.GlobalConfig.Prometheus.Hosts

	model.WrapMsg(c, model.SUCCESS, req)
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