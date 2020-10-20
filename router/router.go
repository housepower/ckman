package router

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/controller"
	"gitlab.eoitek.net/EOI/ckman/database/clickhouse"
)

func InitRouter(router *gin.Engine, config *config.CKManConfig, ck *clickhouse.CkClient) {
	ckController := controller.NewClickHouseController(config, ck)

	groupV1 := router.Group("/api/v1")
	{
		groupV1.POST("/ck/table", ckController.CreateTable)
		groupV1.PUT("/ck/table", ckController.AlterTable)
		groupV1.DELETE("/ck/table", ckController.DeleteTable)
	}
}
