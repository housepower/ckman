package controller

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/pkg/errors"
)

type NodeOverrideController struct {
	Controller
}

func NewNodeOverrideController(wrapfunc Wrapfunc) *NodeOverrideController {
	c := &NodeOverrideController{}
	c.wrapfunc = wrapfunc
	return c
}

func (ctl *NodeOverrideController) resolveCluster(c *gin.Context) (model.CKManClickHouseConfig, string, bool) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")
	if ip == "" {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Errorf("node ip required"))
		return model.CKManClickHouseConfig{}, "", false
	}
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		ctl.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return model.CKManClickHouseConfig{}, "", false
	}
	found := false
	for _, h := range conf.Hosts {
		if h == ip {
			found = true
			break
		}
	}
	if !found {
		ctl.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("ip %s not in cluster %s", ip, clusterName))
		return model.CKManClickHouseConfig{}, "", false
	}
	return conf, ip, true
}

// @Summary 获取节点个性化配置
// @Description 获取指定节点的 config.d/node_override.xml 内容
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name"
// @Param ip query string true "node ip"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"ip":"...","xml":"..."}}"
// @Router /api/v1/ck/node/override/{clusterName} [get]
func (ctl *NodeOverrideController) Get(c *gin.Context) {
	conf, ip, ok := ctl.resolveCluster(c)
	if !ok {
		return
	}
	xmlStr := ""
	if conf.NodeOverrides != nil {
		xmlStr = conf.NodeOverrides[ip]
	}
	ctl.wrapfunc(c, model.E_SUCCESS, model.NodeOverrideRsp{IP: ip, Xml: xmlStr})
}

// @Summary 保存节点个性化配置
// @Description 保存并下发节点 config.d/node_override.xml；空 XML 等同于删除
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name"
// @Param ip query string true "node ip"
// @Param req body model.NodeOverrideReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"reloaded":true}}"
// @Router /api/v1/ck/node/override/{clusterName} [put]
func (ctl *NodeOverrideController) Put(c *gin.Context) {
	var req model.NodeOverrideReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	conf, ip, ok := ctl.resolveCluster(c)
	if !ok {
		return
	}
	pretty, err := common.PrettyXML(req.Xml)
	if err != nil {
		ctl.wrapfunc(c, model.E_INVALID_PARAMS, errors.Wrap(err, "invalid xml"))
		return
	}

	if pretty == "" {
		ctl.doDelete(c, conf, ip)
		return
	}

	reloaded, warning, err := clickhouse.ApplyNodeOverride(&conf, ip, pretty)
	if err != nil {
		ctl.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
		return
	}
	if conf.NodeOverrides == nil {
		conf.NodeOverrides = make(map[string]string)
	}
	conf.NodeOverrides[ip] = pretty
	if err := repository.Ps.UpdateCluster(conf); err != nil {
		log.Logger.Errorf("update cluster after node override apply failed: %v", err)
		// Best-effort rollback: file is on the node but ckman lost record.
		if _, _, rerr := clickhouse.RemoveNodeOverride(&conf, ip); rerr != nil {
			log.Logger.Errorf("rollback node override on %s also failed: %v", ip, rerr)
		}
		ctl.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	ctl.wrapfunc(c, model.E_SUCCESS, model.NodeOverrideApplyRsp{Reloaded: reloaded, Warning: warning})
}

// @Summary 清空节点个性化配置
// @Description 删除节点 config.d/node_override.xml 并 RELOAD CONFIG
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name"
// @Param ip query string true "node ip"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"reloaded":true}}"
// @Router /api/v1/ck/node/override/{clusterName} [delete]
func (ctl *NodeOverrideController) Delete(c *gin.Context) {
	conf, ip, ok := ctl.resolveCluster(c)
	if !ok {
		return
	}
	ctl.doDelete(c, conf, ip)
}

func (ctl *NodeOverrideController) doDelete(c *gin.Context, conf model.CKManClickHouseConfig, ip string) {
	reloaded, warning, err := clickhouse.RemoveNodeOverride(&conf, ip)
	if err != nil {
		ctl.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
		return
	}
	if conf.NodeOverrides != nil {
		delete(conf.NodeOverrides, ip)
	}
	if err := repository.Ps.UpdateCluster(conf); err != nil {
		log.Logger.Errorf("update cluster after node override remove failed: %v", err)
		ctl.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	ctl.wrapfunc(c, model.E_SUCCESS, model.NodeOverrideApplyRsp{Reloaded: reloaded, Warning: warning})
}
