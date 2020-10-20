package server

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/database/clickhouse"
	_ "gitlab.eoitek.net/EOI/ckman/docs"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/router"
	"net/http"
	"time"
)

type ApiServer struct {
	config *config.CKManConfig
	ck     *clickhouse.CkClient
	svr    *http.Server
}

func NewApiServer(config *config.CKManConfig, ck *clickhouse.CkClient) *ApiServer {
	server := &ApiServer{}
	server.config = config
	server.ck = ck
	return server
}

func (server *ApiServer) Start() error {
	r := gin.Default()

	// add log middleware
	r.Use(ginLoggerToFile())

	router.InitRouter(r, server.config, server.ck)

	// http://127.0.0.1:8808/swagger/index.html
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	bind := fmt.Sprintf("%s:%d", server.config.Server.Ip, server.config.Server.Port)
	server.svr = &http.Server{
		Addr: bind,
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
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

func (server *ApiServer) Stop() {
	waitTimeout := time.Duration(time.Second * 10)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	server.svr.Shutdown(ctx)
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
