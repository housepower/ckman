package router

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/controller"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
	"gitlab.eoitek.net/EOI/ckman/service/prometheus"
)

func InitRouterV1(groupV1 *gin.RouterGroup, config *config.CKManConfig, ck *clickhouse.CkService, prom *prometheus.PrometheusService) {
	ckController := controller.NewClickHouseController(config, ck)
	packageController := controller.NewPackageController(config)
	deployController := controller.NewDeployController(config)
	metricController := controller.NewMetricController(config, ck, prom)

	groupV1.POST("/ck/table", ckController.CreateTable)
	groupV1.PUT("/ck/table", ckController.AlterTable)
	groupV1.DELETE("/ck/table", ckController.DeleteTable)
	groupV1.GET("/ck/table", ckController.DescTable)
	groupV1.POST("/package", packageController.Upload)
	groupV1.GET("/package", packageController.List)
	groupV1.DELETE("/package", packageController.Delete)
	groupV1.POST("/deploy/ck", deployController.DeployCk)
	groupV1.GET("/metric/query", metricController.Query)
	groupV1.GET("/metric/query_range", metricController.QueryRange)
}
