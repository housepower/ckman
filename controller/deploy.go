package controller

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/deploy"
	"gitlab.eoitek.net/EOI/ckman/model"
)

type DeployController struct {
	config *config.CKManConfig
}

func NewDeployController(config *config.CKManConfig) *DeployController {
	ck := &DeployController{}
	ck.config = config
	return ck
}

func DeployPackage(d deploy.Deploy, base *deploy.DeployBase, conf interface{}) (int, error) {
	if err := d.Init(base, conf); err != nil {
		return model.INIT_PACKAGE_FAIL, err
	}

	if err := d.Prepare(); err != nil {
		return model.PREPARE_PACKAGE_FAIL, err
	}

	if err := d.Install(); err != nil {
		return model.INSTALL_PACKAGE_FAIL, err
	}

	if err := d.Config(); err != nil {
		return model.CONFIG_PACKAGE_FAIL, err
	}

	if err := d.Start(); err != nil {
		return model.START_PACKAGE_FAIL, err
	}

	if err := d.Check(); err != nil {
		return model.CHECK_PACKAGE_FAIL, err
	}

	return model.SUCCESS, nil
}

// @Summary 部署clickhouse
// @Description 部署clickhouse
// @version 1.0
// @Param req body model.DeployCkReq true "request body"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5011,"msg":"初始化组件失败","data":""}"
// @Failure 200 {string} json "{"code":5012,"msg":"准备组件失败","data":""}"
// @Failure 200 {string} json "{"code":5013,"msg":"安装组件失败","data":""}"
// @Failure 200 {string} json "{"code":5014,"msg":"配置组件失败","data":""}"
// @Failure 200 {string} json "{"code":5015,"msg":"启动组件失败","data":""}"
// @Failure 200 {string} json "{"code":5016,"msg":"检查组件启动状态失败","data":""}"
// @Router /api/v1/deploy/ck [post]
func (d *DeployController) DeployCk(c *gin.Context) {
	var req model.DeployCkReq
	var factory deploy.DeployFactory

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	factory = deploy.CKDeployFacotry{}
	ckDeploy := factory.Create()
	base := &deploy.DeployBase{
		Hosts:     req.Hosts,
		Packages:  req.Packages,
		User:      req.User,
		Password:  req.Password,
		Directory: req.Directory,
	}

	code, err := DeployPackage(ckDeploy, base, &req.ClickHouse)
	if err != nil {
		model.WrapMsg(c, code, model.GetMsg(code), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}
