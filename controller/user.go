package controller

import (
	"regexp"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

const UsernamePath = "username"

var (
	usernameRegex = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{2,31}$`)
	reservedNames = map[string]struct{}{
		common.DefaultAdminName:     {},
		common.InternalOrdinaryName: {},
	}
)

func validUsername(name string) bool {
	return usernameRegex.MatchString(name)
}

func isReservedName(name string) bool {
	_, ok := reservedNames[name]
	return ok
}

func validPolicyForCreate(p string) bool {
	return p == common.GUEST || p == common.ORDINARY
}

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

// @Summary 获取当前登录用户信息
// @Description Get the current logged-in user
// @Tags user
// @Success 200 {object} model.UserListItem
// @Router /api/v1/user/me [get]
func (controller *UserController) Me(c *gin.Context) {
	username := c.GetString("username")
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		if username == common.InternalOrdinaryName {
			// portal sentinel — synthesise response (not in DB)
			controller.wrapfunc(c, model.E_SUCCESS, model.UserListItem{
				Username: username,
				Policy:   common.ORDINARY,
				Enabled:  true,
			})
			return
		}
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, model.UserListItem{
		Username:  user.Username,
		Policy:    user.Policy,
		Enabled:   user.Enabled,
		CreatedAt: user.CreatedAt,
		UpdatedAt: user.UpdatedAt,
	})
}

// @Summary 获取所有用户
// @Description List all users (admin only)
// @Tags user
// @Success 200 {array} model.UserListItem
// @Router /api/v1/users [get]
func (controller *UserController) List(c *gin.Context) {
	users, err := repository.Ps.GetAllUsers()
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	out := make([]model.UserListItem, 0, len(users))
	for _, u := range users {
		out = append(out, model.UserListItem{
			Username:  u.Username,
			Policy:    u.Policy,
			Enabled:   u.Enabled,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		})
	}
	controller.wrapfunc(c, model.E_SUCCESS, out)
}

// @Summary 创建用户
// @Description Create a non-admin user (admin only)
// @Tags user
// @Param req body model.CreateUserReq true "request body"
// @Success 200
// @Router /api/v1/users [post]
func (controller *UserController) Create(c *gin.Context) {
	var req model.CreateUserReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if !validUsername(req.Username) {
		controller.wrapfunc(c, model.E_INVALID_USERNAME, nil)
		return
	}
	if isReservedName(req.Username) {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	if !validPolicyForCreate(req.Policy) {
		controller.wrapfunc(c, model.E_INVALID_POLICY, nil)
		return
	}
	if err := common.VerifyPassword(req.Password); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	md5pw := common.Md5CheckSum(req.Password)
	hash, err := common.HashPassword(md5pw)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	now := time.Now().Unix()
	user := model.CkmanUser{
		Username:     req.Username,
		PasswordHash: hash,
		Policy:       req.Policy,
		Enabled:      req.Enabled,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := repository.Ps.CreateUser(user); err != nil {
		if errors.Is(err, repository.ErrRecordExists) {
			controller.wrapfunc(c, model.E_USER_ALREADY_EXISTS, nil)
			return
		}
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 更新用户角色或启用状态
// @Description Update policy and/or enabled flag (admin only)
// @Tags user
// @Param username path string true "username"
// @Param req body model.UpdateUserReq true "request body"
// @Success 200
// @Router /api/v1/users/{username} [put]
func (controller *UserController) Update(c *gin.Context) {
	username := c.Param(UsernamePath)
	if isReservedName(username) {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	var req model.UpdateUserReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	if req.Policy != "" {
		if !validPolicyForCreate(req.Policy) {
			controller.wrapfunc(c, model.E_INVALID_POLICY, nil)
			return
		}
		user.Policy = req.Policy
	}
	if req.Enabled != nil {
		user.Enabled = *req.Enabled
	}
	user.UpdatedAt = time.Now().Unix()
	if err := repository.Ps.UpdateUser(user); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 删除用户
// @Description Hard-delete a user and revoke their tokens (admin only)
// @Tags user
// @Param username path string true "username"
// @Success 200
// @Router /api/v1/users/{username} [delete]
func (controller *UserController) Delete(c *gin.Context) {
	username := c.Param(UsernamePath)
	if isReservedName(username) {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	if err := repository.Ps.DeleteUser(username); err != nil {
		if errors.Is(err, repository.ErrRecordNotFound) {
			controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
			return
		}
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	revokeTokensFor(username)
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// revokeTokensFor walks the in-memory TokenCache and removes entries whose JWT
// claims.Name equals username. Used after deleting a user account.
func revokeTokensFor(username string) {
	for token := range TokenCache.Items() {
		j := common.NewJWT()
		claims, code := j.ParserToken(token)
		if code != model.E_SUCCESS {
			continue
		}
		if claims.Name == username {
			TokenCache.Delete(token)
		}
	}
}

// @Summary 修改自己的密码
// @Description Change current user's own password (requires old password)
// @Tags user
// @Param req body model.ChangeMyPasswordReq true "request body"
// @Success 200
// @Router /api/v1/user/password [put]
func (controller *UserController) ChangeMyPassword(c *gin.Context) {
	username := c.GetString("username")
	if username == common.InternalOrdinaryName {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	var req model.ChangeMyPasswordReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	if !common.ComparePassword(user.PasswordHash, common.Md5CheckSum(req.OldPassword)) {
		controller.wrapfunc(c, model.E_OLD_PASSWORD_MISMATCH, nil)
		return
	}
	if err := common.VerifyPassword(req.NewPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}
	hash, err := common.HashPassword(common.Md5CheckSum(req.NewPassword))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	user.PasswordHash = hash
	user.UpdatedAt = time.Now().Unix()
	if err := repository.Ps.UpdateUser(user); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 重置他人密码
// @Description Admin resets another user's password without needing the old password
// @Tags user
// @Param username path string true "username"
// @Param req body model.ResetPasswordReq true "request body"
// @Success 200
// @Router /api/v1/users/{username}/password [put]
func (controller *UserController) ResetPassword(c *gin.Context) {
	username := c.Param(UsernamePath)
	var req model.ResetPasswordReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	if err := common.VerifyPassword(req.NewPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}
	hash, err := common.HashPassword(common.Md5CheckSum(req.NewPassword))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	user.PasswordHash = hash
	user.UpdatedAt = time.Now().Unix()
	if err := repository.Ps.UpdateUser(user); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
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
