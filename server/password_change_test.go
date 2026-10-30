package server

import (
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/repository/sqlite"
)

func TestInitialPasswordRestrictsAPIUntilChanged(t *testing.T) {
	log.InitLoggerConsole()
	sp := sqlite.NewSQLitePersistent()
	if err := sp.Init(sqlite.LocalConfig{ConfigDir: t.TempDir(), ConfigFile: "users.db"}); err != nil {
		t.Fatal(err)
	}
	previous := repository.Ps
	repository.Ps = sp
	t.Cleanup(func() { repository.Ps = previous })
	user := model.CkmanUser{Username: common.DefaultAdminName, PasswordHash: "hash", Policy: common.ADMIN, Enabled: true, MustChangePassword: true}
	if err := sp.CreateUser(user); err != nil {
		t.Fatal(err)
	}

	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("username", common.DefaultAdminName) })
	r.Use(ginRequirePasswordChange())
	r.GET("/api/v1/ck/cluster", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	r.GET("/api/v1/user/me", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	r.PUT("/api/v1/user/password", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	r.PUT("/api/logout", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	check := func(method, path string, wantStatus int, wantCode string) {
		t.Helper()
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		if w.Code != wantStatus {
			t.Fatalf("%s %s: status %d, want %d", method, path, w.Code, wantStatus)
		}
		if wantCode != "" {
			var rsp struct {
				RetCode string `json:"retCode"`
			}
			if err := stdjson.Unmarshal(w.Body.Bytes(), &rsp); err != nil || rsp.RetCode != wantCode {
				t.Fatalf("%s %s: code %q, error %v", method, path, rsp.RetCode, err)
			}
		}
	}
	check("GET", "/api/v1/ck/cluster", http.StatusOK, model.E_PASSWORD_CHANGE_REQUIRED)
	check("GET", "/api/v1/user/me", http.StatusNoContent, "")
	check("PUT", "/api/v1/user/password", http.StatusNoContent, "")
	check("PUT", "/api/logout", http.StatusNoContent, "")

	user.MustChangePassword = false
	if err := sp.UpdateUser(user); err != nil {
		t.Fatal(err)
	}
	check("GET", "/api/v1/ck/cluster", http.StatusNoContent, "")
}
