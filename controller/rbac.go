package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
)

type RBACController struct {
	wrapfunc Wrapfunc
}

func NewRBACController(wrapfunc Wrapfunc) *RBACController {
	return &RBACController{wrapfunc: wrapfunc}
}

// CreateUser godoc
// @Summary 创建CH用户
// @Tags rbac
// @Accept json
// @Param clusterName path string true "cluster name"
// @Param req body model.RbacUserCreateReq true "user"
// @Router /api/v1/ck/rbac/user/{clusterName} [post]
func (controller *RBACController) CreateUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacUserCreateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if err := clickhouse.RbacCreateUser(clusterName, req); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// ListUsers godoc
// @Summary 列出CH用户
// @Tags rbac
// @Param clusterName path string true "cluster name"
// @Param name query string false "name filter"
// @Router /api/v1/ck/rbac/user/{clusterName} [get]
func (controller *RBACController) ListUsers(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	users, err := clickhouse.RbacListUsers(clusterName, c.Query("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, users)
}

// GetUser godoc
// @Summary 查询单个CH用户
// @Tags rbac
// @Router /api/v1/ck/rbac/user/{clusterName}/{name} [get]
func (controller *RBACController) GetUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	user, err := clickhouse.RbacGetUser(clusterName, c.Param("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, user)
}

// AlterUser godoc
// @Summary 修改CH用户
// @Tags rbac
// @Router /api/v1/ck/rbac/user/{clusterName}/{name} [put]
func (controller *RBACController) AlterUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacUserAlterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if err := clickhouse.RbacAlterUser(clusterName, c.Param("name"), req); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// DropUser godoc
// @Summary 删除CH用户
// @Tags rbac
// @Router /api/v1/ck/rbac/user/{clusterName}/{name} [delete]
func (controller *RBACController) DropUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	if err := clickhouse.RbacDropUser(clusterName, c.Param("name")); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// CreateProfile godoc
// @Summary 创建settings profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName} [post]
func (controller *RBACController) CreateProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacProfileCreateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if err := clickhouse.RbacCreateProfile(clusterName, req); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// ListProfiles godoc
// @Summary 列出settings profile
// @Tags rbac
// @Param name query string false "name filter"
// @Router /api/v1/ck/rbac/profile/{clusterName} [get]
func (controller *RBACController) ListProfiles(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	profiles, err := clickhouse.RbacListProfiles(clusterName, c.Query("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, profiles)
}

// GetProfile godoc
// @Summary 查询单个profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName}/{name} [get]
func (controller *RBACController) GetProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	profile, err := clickhouse.RbacGetProfile(clusterName, c.Param("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, profile)
}

// AlterProfile godoc
// @Summary 修改profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName}/{name} [put]
func (controller *RBACController) AlterProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacProfileCreateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	req.Name = c.Param("name")
	if err := clickhouse.RbacAlterProfile(clusterName, req); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// DropProfile godoc
// @Summary 删除profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName}/{name} [delete]
func (controller *RBACController) DropProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	if err := clickhouse.RbacDropProfile(clusterName, c.Param("name")); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
