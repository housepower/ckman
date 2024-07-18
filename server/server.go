package server

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-errors/errors"
	jsoniter "github.com/json-iterator/go"

	"github.com/patrickmn/go-cache"

	"github.com/arl/statsviz"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/controller"
	_ "github.com/housepower/ckman/docs"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/router"
	"github.com/housepower/ckman/server/enforce"
	"github.com/housepower/ckman/service/prometheus"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const ENV_CKMAN_SWAGGER string = "ENV_CKMAN_SWAGGER"

type ApiServer struct {
	config *config.CKManConfig
	svr    *http.Server
	signal chan os.Signal
	fs     embed.FS
}

func NewApiServer(config *config.CKManConfig, signal chan os.Signal, fs embed.FS) *ApiServer {
	server := &ApiServer{}
	server.config = config
	server.signal = signal
	server.fs = fs
	return server
}

func (server *ApiServer) Start() error {
	gin.SetMode(gin.ReleaseMode)
	// Create gin engine with customized log and recovery middlewares.
	r := gin.New()
	r.Use(ginLoggerToFile())
	r.Use(gin.CustomRecoveryWithWriter(nil, handlePanic))

	controller.TokenCache = cache.New(time.Duration(server.config.Server.SessionTimeout)*time.Second, time.Minute)
	userController := controller.NewUserController(server.config, router.WrapMsg)

	r.Use(Serve("/", EmbedFolder(server.fs, "static/dist")))
	r.NoRoute(func(c *gin.Context) {
		data, err := server.fs.ReadFile("static/dist/index.html")
		if err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", data)
	})
	if !server.config.Server.SwaggerEnable {
		_ = os.Setenv(ENV_CKMAN_SWAGGER, "disabled")
	} else {
		_ = os.Unsetenv(ENV_CKMAN_SWAGGER)
	}
	// http://127.0.0.1:8808/swagger/index.html
	r.GET("/swagger/*any", ginSwagger.DisablingWrapHandler(swaggerFiles.Handler, ENV_CKMAN_SWAGGER))

	// http://127.0.0.1:8808/debug/pprof/
	if server.config.Server.Pprof {
		pprof.Register(r)
	}

	// http://127.0.0.1:8808/debug/statsviz
	r.GET("/debug/statsviz/*filepath", func(context *gin.Context) {
		if context.Param("filepath") == "/ws" {
			statsviz.Ws(context.Writer, context.Request)
			return
		}
		statsviz.IndexAtRoot("/debug/statsviz").ServeHTTP(context.Writer, context.Request)
	})

	// prometheus http_sd_config
	r.GET("discovery/:schema", PromHttpSD)

	groupApi := r.Group("/api")
	groupApi.POST("/login", userController.Login)
	// add authenticate middleware for /api
	common.LoadUsers(filepath.Dir(server.config.ConfigFile))
	groupApi.Use(ginJWTAuth())
	groupApi.Use(ginRefreshTokenExpires())
	groupApi.Use(ginEnforce())
	groupApi.PUT("/logout", userController.Logout)
	groupV1 := groupApi.Group("/v1")
	router.InitRouterV1(groupV1, server.config, server.signal)

	groupV2 := groupApi.Group("/v2")
	router.InitRouterV2(groupV2, server.config, server.signal)

	bind := fmt.Sprintf(":%d", server.config.Server.Port)
	server.svr = &http.Server{
		Addr:         bind,
		WriteTimeout: time.Second * 3600,
		ReadTimeout:  time.Second * 3600,
		IdleTimeout:  time.Second * 3600,
		Handler:      r,
	}

	if server.config.Server.Https {
		go func() {
			if err := server.svr.ListenAndServeTLS(server.config.Server.CertFile, server.config.Server.KeyFile); err != nil && err != http.ErrServerClosed {
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

// Log runtime error stack to make debug easy.
func handlePanic(c *gin.Context, err interface{}) {
	log.Logger.Errorf("server panic: %+v\n%v", err, string(debug.Stack()))
	router.WrapMsg(c, model.E_UNKNOWN, err)
}

// Replace gin.Logger middleware to customize log format.
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
		msg := fmt.Sprintf("| %3d | %13v | %15s | %s | %s",
			statusCode,
			latencyTime,
			clientIP,
			reqMethod,
			reqUri)
		if statusCode == 200 {
			log.Logger.Info(msg)
		} else {
			log.Logger.Error(msg)
		}
	}
}

func ginJWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// intercept token from unified portal
		// it has higher priority than jwt
		uEnc := c.Request.Header.Get("userToken")
		if uEnc != "" {
			//request from unified portal
			var rsaEncrypt common.RSAEncryption
			decode, err := rsaEncrypt.Decode([]byte(uEnc), config.GlobalConfig.Server.PublicKey)
			if err != nil {
				router.WrapMsg(c, model.E_JWT_TOKEN_INVALID, nil)
				c.Abort()
				return
			}

			var userToken common.UserTokenModel
			err = json.Unmarshal(decode, &userToken)
			if err != nil {
				router.WrapMsg(c, model.E_JWT_TOKEN_INVALID, nil)
				c.Abort()
				return
			}
			if time.Now().UnixNano()/1e6-userToken.Timestamp > userToken.Duration*1000 {
				router.WrapMsg(c, model.E_JWT_TOKEN_EXPIRED, nil)
				c.Abort()
				return
			}
			//c.Set("username", userToken.UserId)
			c.Set("username", common.InternalOrdinaryName)
			return
		}

		// jwt
		token := c.Request.Header.Get("token")
		if token == "" {
			router.WrapMsg(c, model.E_JWT_TOKEN_NONE, nil)
			c.Abort()
			return
		}

		j := common.NewJWT()
		claims, code := j.ParserToken(token)
		if code != model.E_SUCCESS {
			router.WrapMsg(c, code, nil)
			c.Abort()
			return
		}

		// Verify Expires
		if _, ok := controller.TokenCache.Get(token); !ok {
			router.WrapMsg(c, model.E_JWT_TOKEN_EXPIRED, nil)
			c.Abort()
			return
		}

		// Verify client ip
		// proxy with ngnix, will get client ip from x-forwarded-for
		// refs: https://nginx.org/en/docs/http/ngx_http_realip_module.html
		// refs: https://www.cnblogs.com/mypath/articles/5239687.html
		/*
			nginx config example:
						server {
				        	listen       80;
				        	server_name  net.eoitek.ckman.com;

				        	location / {
				                        index  index.html index.htm;
				                        proxy_pass http://192.168.110.8:8808;
				                        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
				        	}
				    }

		*/
		clientIps := c.Request.Header.Get("X-Forwarded-For")
		if clientIps != "" {
			clientIp := strings.Split(clientIps, ",")[0]
			log.Logger.Debugf("maybe set proxy, the real ip is %s", clientIp)
			if clientIp == claims.ClientIP {
				c.Set("claims", claims)
				c.Set("token", token)
				c.Set("username", claims.Name)
				return
			}
		}
		if claims.ClientIP != c.ClientIP() {
			err := errors.Errorf("cliams.ClientIP: %s, c.ClientIP:%s", claims.ClientIP, c.ClientIP())
			router.WrapMsg(c, model.E_JWT_TOKEN_EXPIRED, err)
			c.Abort()
			return
		}

		c.Set("claims", claims)
		c.Set("token", token)
		c.Set("username", claims.Name)
	}
}

func ginRefreshTokenExpires() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if value, exists := c.Get("token"); exists {
			token := value.(string)
			if token != "" {
				controller.TokenCache.SetDefault(token, time.Now().Add(time.Second*time.Duration(config.GlobalConfig.Server.SessionTimeout)).Unix())
			}
		}
	}
}

func PromHttpSD(c *gin.Context) {
	var clusters []model.CKManClickHouseConfig
	schema := c.Param("schema")
	if schema != "clickhouse" && schema != "zookeeper" && schema != "node" {
		router.WrapMsg(c, model.E_INVALID_PARAMS, fmt.Errorf("%s is not a valid schema", schema))
		return
	}
	clusterName := c.Query("cluster")
	if clusterName == "" {
		all, err := repository.Ps.GetAllClusters()
		if err != nil {
			log.Logger.Error(err)
			return
		}
		for _, v := range all {
			clusters = append(clusters, v)
		}
	} else {
		cluster, err := repository.Ps.GetClusterbyName(clusterName)
		if err != nil {
			log.Logger.Error(err)
			return
		}
		clusters = append(clusters, cluster)
	}
	objs := prometheus.GetObjects(clusters)
	c.JSON(http.StatusOK, objs[schema])
}

func ginEnforce() gin.HandlerFunc {
	return func(c *gin.Context) {
		username := c.GetString("username")
		ok := enforce.Enforce(username, c.Request.URL.RequestURI(), c.Request.Method)
		if !ok {
			err := fmt.Errorf("permission denied: username [%s]", username)
			router.WrapMsg(c, model.E_USER_VERIFY_FAIL, err)
			c.Abort()
		}
	}
}
