package controller

import (
	"bytes"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/repository/sqlite"
	"github.com/patrickmn/go-cache"
)

func TestInitialAdminMustChangePassword(t *testing.T) {
	log.InitLoggerConsole()
	sp := sqlite.NewSQLitePersistent()
	if err := sp.Init(sqlite.LocalConfig{ConfigDir: t.TempDir(), ConfigFile: "users.db"}); err != nil {
		t.Fatal(err)
	}
	previous := repository.Ps
	repository.Ps = sp
	t.Cleanup(func() { repository.Ps = previous })
	const initial = "InitialPass123!"
	hash, err := common.HashPassword(common.Md5CheckSum(initial))
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.CreateUser(model.CkmanUser{Username: common.DefaultAdminName, PasswordHash: hash, Policy: common.ADMIN, Enabled: true, MustChangePassword: true}); err != nil {
		t.Fatal(err)
	}
	TokenCache = cache.New(time.Hour, time.Minute)
	var code string
	var entity interface{}
	controller := NewUserController(&config.CKManConfig{}, func(_ *gin.Context, c string, e interface{}) { code, entity = c, e })
	controller.config.Server.SessionTimeout = 3600
	request := func(body string) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/api/login", bytes.NewBufferString(body))
		c.Set("username", common.DefaultAdminName)
		return c
	}
	controller.Login(request(`{"username":"ckman","password":"` + common.Md5CheckSum(initial) + `"}`))
	if code != model.E_SUCCESS || !entity.(model.LoginRsp).MustChangePassword {
		t.Fatal("first login did not require password change")
	}
	controller.ChangeMyPassword(request(`{"old_password":"InitialPass123!","new_password":"InitialPass123!"}`))
	if code != model.E_DATA_CHECK_FAILED {
		t.Fatal("same password was accepted")
	}
	controller.ChangeMyPassword(request(`{"old_password":"InitialPass123!","new_password":"ChangedPass123!"}`))
	if code != model.E_SUCCESS {
		t.Fatalf("password change failed: %s", code)
	}
	user, err := sp.GetUserByName(common.DefaultAdminName)
	if err != nil || user.MustChangePassword || !common.ComparePassword(user.PasswordHash, common.Md5CheckSum("ChangedPass123!")) {
		t.Fatal("new password or required-change flag was not persisted")
	}
	controller.Login(request(`{"username":"ckman","password":"` + common.Md5CheckSum("ChangedPass123!") + `"}`))
	if code != model.E_SUCCESS || entity.(model.LoginRsp).MustChangePassword {
		t.Fatal("subsequent login still requires password change")
	}
}
