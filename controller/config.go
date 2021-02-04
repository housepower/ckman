package controller

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/model"
	"os"
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
// @Failure 200 {string} json "{"code":400,"msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":5070,"msg":"update config failed","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Router /api/v1/config [put]
func (cf *ConfigController) UpdateConfig(c *gin.Context) {
	var req model.UpdateConfigReq

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err.Error())
		return
	}

	config.GlobalConfig.Prometheus.Hosts = req.Prometheus

	if err := config.MarshConfigFile(); err != nil {
		model.WrapMsg(c, model.UPDATE_CONFIG_FAIL, model.GetMsg(c, model.UPDATE_CONFIG_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
	cf.signal <- syscall.SIGHUP
}

// @Summary Get Config
// @Description Get Config
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"code":200,"msg":"ok","data":{"peers":null,"prometheus":["192.168.101.105:19090"],"alertManagers":null}}"
// @Router /api/v1/config [get]
func (cf *ConfigController) GetConfig(c *gin.Context) {
	var req model.UpdateConfigReq

	req.Prometheus = config.GlobalConfig.Prometheus.Hosts

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), req)
}
