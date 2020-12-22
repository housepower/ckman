package server

import (
	"context"
	"fmt"
	"github.com/patrickmn/go-cache"
	"io"
	"net/http"
	"os"
	"time"

	static "github.com/choidamdam/gin-static-pkger"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/markbates/pkger"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
	"gitlab.eoitek.net/EOI/ckman/common"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/controller"
	_ "gitlab.eoitek.net/EOI/ckman/docs"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/router"
	"gitlab.eoitek.net/EOI/ckman/service/prometheus"
)

type ApiServer struct {
	config *config.CKManConfig
	prom   *prometheus.PrometheusService
	svr    *http.Server
	signal chan os.Signal
}

func NewApiServer(config *config.CKManConfig, prom *prometheus.PrometheusService, signal chan os.Signal) *ApiServer {
	server := &ApiServer{}
	server.config = config
	server.prom = prom
	server.signal = signal
	return server
}

func (server *ApiServer) Start() error {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// add log middleware
	r.Use(ginLoggerToFile())

	controller.TokenCache = cache.New(time.Duration(server.config.Server.SessionTimeout)*time.Second, time.Minute)
	userController := controller.NewUserController(server.config)

	// https://github.com/gin-gonic/gin/issues/1048
	// How do you solve vue.js HTML5 History Mode?
	_ = pkger.Dir("/frontend/dist")
	r.Use(static.Serve("/", static.LocalFile("/frontend/dist", false)))
	homepage := embedStaticHandler("/frontend/dist/index.html", "text/html;charset=utf-8")
	r.NoRoute(homepage)

	// http://127.0.0.1:8808/swagger/index.html
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// http://127.0.0.1:8808/debug/pprof/
	if server.config.Server.Pprof {
		pprof.Register(r)
	}

	groupApi := r.Group("/api")
	groupApi.POST("/login", userController.Login)
	// add authenticate middleware for /api
	groupApi.Use(ginJWTAuth())
	groupApi.Use(ginRefreshTokenExpires())
	groupApi.PUT("/logout", userController.Logout)
	groupV1 := groupApi.Group("/v1")
	router.InitRouterV1(groupV1, server.config, server.prom, server.signal)

	bind := fmt.Sprintf("%s:%d", server.config.Server.Ip, server.config.Server.Port)
	server.svr = &http.Server{
		Addr:         bind,
		WriteTimeout: time.Second * 300,
		ReadTimeout:  time.Second * 300,
		IdleTimeout:  time.Second * 60,
		Handler:      r,
	}

	if server.config.Server.Https {
		// FIXME certFile and keyFile are incorrect
		go func() {
			if err := server.svr.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				log.Logger.Fatalf("start https server fail: %s", err.Error())
			}
		}()
	} else {
		go func() {
			if err := server.svr.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Logger.Fatalf("start http server start fail: %s", err.Error())
			}
		}()
	}

	return nil
}

func (server *ApiServer) Stop() error {
	waitTimeout := time.Duration(time.Second * 10)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	return server.svr.Shutdown(ctx)
}

func embedStaticHandler(embedPath, contentType string) gin.HandlerFunc {
	return func(c *gin.Context) {
		f, err := pkger.Open(embedPath)
		if err != nil {
			log.Logger.Errorf("failed to open embed static file %s", embedPath)
			return
		}
		defer f.Close()
		c.Status(http.StatusOK)
		c.Header("Content-Type", contentType)
		if _, err := io.Copy(c.Writer, f); err != nil {
			log.Logger.Errorf("failed to copy embed static file %s", embedPath)
		}
	}
}

func ginLoggerToFile() gin.HandlerFunc {
	return func(c *gin.Context) {
		// start time
		startTime := time.Now()
		// Processing request
		c.Next()
		// End time
		endTime := time.Now()
		// execution time
		latencyTime := endTime.Sub(startTime)
		// Request mode
		reqMethod := c.Request.Method
		// Request routing
		reqUri := c.Request.RequestURI
		// Status code
		statusCode := c.Writer.Status()
		// Request IP
		clientIP := c.ClientIP()
		// Log format
		if statusCode == model.SUCCESS {
			log.Logger.Infof("| %3d | %13v | %15s | %s | %s",
				statusCode,
				latencyTime,
				clientIP,
				reqMethod,
				reqUri,
			)
		} else {
			log.Logger.Errorf("| %3d | %13v | %15s | %s | %s",
				statusCode,
				latencyTime,
				clientIP,
				reqMethod,
				reqUri,
			)
		}
	}
}

func ginJWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.Request.Header.Get("token")
		if token == "" {
			model.WrapMsg(c, model.JWT_TOKEN_NONE, model.GetMsg(model.JWT_TOKEN_NONE), nil)
			c.Abort()
			return
		}

		j := common.NewJWT()
		claims, code := j.ParserToken(token)
		if code != model.SUCCESS {
			model.WrapMsg(c, code, model.GetMsg(code), nil)
			c.Abort()
			return
		}

		// Verify Expires
		if _, ok := controller.TokenCache.Get(token); !ok {
			model.WrapMsg(c, model.JWT_TOKEN_EXPIRED, model.GetMsg(model.JWT_TOKEN_EXPIRED), nil)
			c.Abort()
			return
		}

		// Verify client ip
		if claims.ClientIP != c.ClientIP() {
			model.WrapMsg(c, model.JWT_TOKEN_IP_MISMATCH, model.GetMsg(model.JWT_TOKEN_IP_MISMATCH), nil)
			c.Abort()
			return
		}

		c.Set("claims", claims)
		c.Set("token", token)
	}
}

func ginRefreshTokenExpires() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if value, exists := c.Get("token"); exists {
			token := value.(string)
			if token != "" {
				controller.TokenCache.SetDefault(token, time.Now().Add(time.Second * time.Duration(config.GlobalConfig.Server.SessionTimeout)).Unix())
			}
		}
	}
}
