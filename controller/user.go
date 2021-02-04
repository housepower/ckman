package controller

import (
	"io/ioutil"
	"path"
	"path/filepath"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"gitlab.eoitek.net/EOI/ckman/common"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/model"
)

var TokenCache *cache.Cache

type UserController struct {
	config *config.CKManConfig
}

func NewUserController(config *config.CKManConfig) *UserController {
	ck := &UserController{}
	ck.config = config
	return ck
}

// @Summary Login
// @Description Login
// @version 1.0
// @Param req body model.LoginReq true "request body"
// @Failure 200 {string} json "{"code":400,"msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":5030,"msg":"user verify failed","data":""}"
// @Failure 200 {string} json "{"code":5031,"msg":"get user and password failed","data":""}"
// @Failure 200 {string} json "{"code":5032,"msg":"password verify failed","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":{"username":"ckman","token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}}"
// @Router /api/login [post]
func (d *UserController) Login(c *gin.Context) {
	var req model.LoginReq
	c.Request.Header.Get("")
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err.Error())
		return
	}

	if req.Username != common.DefaultUserName {
		model.WrapMsg(c, model.USER_VERIFY_FAIL, model.GetMsg(c, model.USER_VERIFY_FAIL), nil)
		return
	}

	passwordFile := path.Join(filepath.Dir(d.config.ConfigFile), "password")
	data, err := ioutil.ReadFile(passwordFile)
	if err != nil {
		model.WrapMsg(c, model.GET_USER_PASSWORD_FAIL, model.GetMsg(c, model.GET_USER_PASSWORD_FAIL), err.Error())
		return
	}

	if pass := common.ComparePassword(string(data), req.Password); !pass {
		model.WrapMsg(c, model.PASSWORD_VERIFY_FAIL, model.GetMsg(c, model.PASSWORD_VERIFY_FAIL), nil)
		return
	}

	j := common.NewJWT()
	claims := common.CustomClaims{
		StandardClaims: jwt.StandardClaims{
			IssuedAt: time.Now().Unix(),
			//ExpiresAt: time.Now().Add(time.Second * time.Duration(d.config.Server.SessionTimeout)).Unix(),
		},
		Name:     common.DefaultUserName,
		ClientIP: c.ClientIP(),
	}
	token, err := j.CreateToken(claims)
	if err != nil {
		model.WrapMsg(c, model.CREAT_TOKEN_FAIL, model.GetMsg(c, model.CREAT_TOKEN_FAIL), err.Error())
		return
	}

	rsp := model.LoginRsp{
		Username: req.Username,
		Token:    token,
	}
	TokenCache.SetDefault(token, time.Now().Add(time.Second * time.Duration(d.config.Server.SessionTimeout)).Unix())

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), rsp)
}

// @Summary Logout
// @Description Logout
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Router /api/logout [put]
func (d *UserController) Logout(c *gin.Context) {
	if value, exists := c.Get("token"); exists {
		token := value.(string)
		TokenCache.Delete(token)
		c.Set("token", "")
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}
