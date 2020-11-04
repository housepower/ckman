package router

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/controller"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
)

func InitRouter(router *gin.Engine, config *config.CKManConfig, ck *clickhouse.CkService) {
	ckController := controller.NewClickHouseController(config, ck)
	packageController := controller.NewPackageController(config)
	deployController := controller.NewDeployController(config)
	userController := controller.NewUserController(config)

	groupV1 := router.Group("/api/v1")
	{
		groupV1.POST("/login", userController.Login)
		groupV1.POST("/ck/table", ckController.CreateTable)
		groupV1.PUT("/ck/table", ckController.AlterTable)
		groupV1.DELETE("/ck/table", ckController.DeleteTable)
		groupV1.POST("/package", packageController.Upload)
		groupV1.GET("/package", packageController.List)
		groupV1.DELETE("/package", packageController.Delete)
		groupV1.POST("/deploy/ck", deployController.DeployCk)
	}
}
