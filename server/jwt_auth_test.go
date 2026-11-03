package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/controller"
	ckmanlog "github.com/housepower/ckman/log"
	"github.com/patrickmn/go-cache"
	"go.uber.org/zap"
)

func TestGinJWTAuthSkipClientIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previousConfig := config.GlobalConfig
	previousLogger := ckmanlog.Logger
	ckmanlog.Logger = zap.NewNop().Sugar()
	t.Cleanup(func() {
		config.GlobalConfig = previousConfig
		ckmanlog.Logger = previousLogger
	})

	jwtService := common.NewJWT()
	token, err := jwtService.CreateToken(common.CustomClaims{
		StandardClaims: jwt.StandardClaims{IssuedAt: time.Now().Unix()},
		Name:           common.DefaultAdminName,
		ClientIP:       "10.0.0.1",
	})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	for _, tt := range []struct {
		name        string
		skip        bool
		wantReached bool
	}{
		{name: "client IP check enabled", skip: false, wantReached: false},
		{name: "client IP check skipped", skip: true, wantReached: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config.GlobalConfig.Server.JWTSkipClientIP = tt.skip
			controller.TokenCache = cache.New(time.Hour, time.Minute)
			controller.TokenCache.SetDefault(token, time.Now().Add(time.Hour).Unix())

			reached := false
			router := gin.New()
			router.Use(ginJWTAuth())
			router.GET("/api/test", func(c *gin.Context) {
				reached = true
				c.Status(http.StatusNoContent)
			})

			req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
			req.Header.Set("token", token)
			req.RemoteAddr = "192.0.2.10:1234"
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)

			if reached != tt.wantReached {
				t.Fatalf("handler reached = %v, want %v", reached, tt.wantReached)
			}
		})
	}
}
