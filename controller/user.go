package controller

import (
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
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

// @Summary Login
// @Description Login
// @version 1.0
// @Param req body model.LoginReq true "request body"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5030","retMsg":"user verify failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5031","retMsg":"get user and password failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5032","retMsg":"password verify failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":{"username":"ckman","token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}}"
// @Router /api/login [post]
func (controller *UserController) Login(c *gin.Context) {
	var req model.LoginReq
	c.Request.Header.Get("")
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	if req.Username != common.DefaultUserName {
		controller.wrapfunc(c, model.E_USER_VERIFY_FAIL, nil)
		return
	}

	passwordFile := path.Join(filepath.Dir(controller.config.ConfigFile), "password")
	data, err := os.ReadFile(passwordFile)
	if err != nil {
		controller.wrapfunc(c, model.E_GET_USER_PASSWORD_FAIL, err)
		return
	}

	if pass := common.ComparePassword(string(data), req.Password); !pass {
		controller.wrapfunc(c, model.E_PASSWORD_VERIFY_FAIL, nil)
		return
	}

	j := common.NewJWT()
	claims := common.CustomClaims{
		StandardClaims: jwt.StandardClaims{
			IssuedAt: time.Now().Unix(),
			// ExpiresAt: time.Now().Add(time.Second * time.Duration(d.config.Server.SessionTimeout)).Unix(),
		},
		Name:     common.DefaultUserName,
		ClientIP: c.ClientIP(),
	}
	token, err := j.CreateToken(claims)
	if err != nil {
		controller.wrapfunc(c, model.E_CREAT_TOKEN_FAIL, err)
		return
	}

	rsp := model.LoginRsp{
		Username: req.Username,
		Token:    token,
	}
	TokenCache.SetDefault(token, time.Now().Add(time.Second*time.Duration(controller.config.Server.SessionTimeout)).Unix())

	controller.wrapfunc(c, model.E_SUCCESS, rsp)
}

// @Summary Logout
// @Description Logout
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/logout [put]
func (controller *UserController) Logout(c *gin.Context) {
	if value, exists := c.Get("token"); exists {
		token := value.(string)
		TokenCache.Delete(token)
		c.Set("token", "")
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
