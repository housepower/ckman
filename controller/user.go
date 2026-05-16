package controller

import (
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/patrickmn/go-cache"
)

var TokenCache *cache.Cache

type UserController struct {
	Controller
	config *config.CKManConfig
}

func NewUserController(config *config.CKManConfig, wrapfunc Wrapfunc) *UserController {
	uc := &UserController{}
	uc.config = config
	uc.wrapfunc = wrapfunc
	return uc
}

// @Summary 用户登录
// @Description Login
// @Security ApiKeyAuth
// @Tags user
// @Accept  json
// @Param req body model.LoginReq true "request body"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"成功","data":null}"
// @Failure 200 {string} json "{"retCode":"5000", "retMsg":"参数错误", "data":null}"
// @Failure 200 {string} json "{"retCode":"5030", "retMsg":"用户校验失败", "data":null}"
// @Failure 200 {string} json "{"retCode":"5031", "retMsg":"密码校验失败", "data":null}"
// @Router /api/login [post]
func (controller *UserController) Login(c *gin.Context) {
	var req model.LoginReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	user, err := repository.Ps.GetUserByName(req.Username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_VERIFY_FAIL, err)
		return
	}
	if !user.Enabled {
		controller.wrapfunc(c, model.E_LOGIN_DISABLED, nil)
		return
	}
	if pass := common.ComparePassword(user.PasswordHash, req.Password); !pass {
		controller.wrapfunc(c, model.E_PASSWORD_VERIFY_FAIL, nil)
		return
	}

	j := common.NewJWT()
	claims := common.CustomClaims{
		StandardClaims: jwt.StandardClaims{
			IssuedAt: time.Now().Unix(),
		},
		Name:     req.Username,
		ClientIP: c.ClientIP(),
	}
	token, err := j.CreateToken(claims)
	if err != nil {
		controller.wrapfunc(c, model.E_CREAT_TOKEN_FAIL, err)
		return
	}

	rsp := model.LoginRsp{
		Username: user.Username,
		Token:    token,
		Policy:   user.Policy,
		Enabled:  user.Enabled,
	}
	TokenCache.SetDefault(token,
		time.Now().Add(time.Second*time.Duration(controller.config.Server.SessionTimeout)).Unix())

	controller.wrapfunc(c, model.E_SUCCESS, rsp)
}

// @Summary 退出登录
// @Description Logout
// @Security ApiKeyAuth
// @Tags user
// @Accept  json
// @Success 200 {string} json "{"retCode":"0000","retMsg":"成功","data":null}"
// @Router /api/logout [put]
func (controller *UserController) Logout(c *gin.Context) {
	if value, exists := c.Get("token"); exists {
		token := value.(string)
		TokenCache.Delete(token)
		c.Set("token", "")
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
