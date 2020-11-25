package router

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/controller"
	"gitlab.eoitek.net/EOI/ckman/service/prometheus"
)

func InitRouterV1(groupV1 *gin.RouterGroup, config *config.CKManConfig, prom *prometheus.PrometheusService) {
	ckController := controller.NewClickHouseController()
	packageController := controller.NewPackageController(config)
	deployController := controller.NewDeployController(config)
	metricController := controller.NewMetricController(config, prom)

	groupV1.POST("/ck/cluster", ckController.ImportCk)
	groupV1.PUT("/ck/cluster", ckController.UpdateCk)
	groupV1.GET("/ck/cluster", ckController.GetCk)
	groupV1.DELETE(fmt.Sprintf("/ck/cluster/:%s", controller.ClickHouseClusterPath), ckController.DeleteCk)
	groupV1.POST(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.CreateTable)
	groupV1.PUT(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.AlterTable)
	groupV1.DELETE(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DeleteTable)
	groupV1.GET(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DescTable)
	groupV1.GET(fmt.Sprintf("/ck/query/:%s", controller.ClickHouseClusterPath), ckController.QueryInfo)
	groupV1.POST("/package", packageController.Upload)
	groupV1.GET("/package", packageController.List)
	groupV1.DELETE("/package", packageController.Delete)
	groupV1.POST("/deploy/ck", deployController.DeployCk)
	groupV1.GET("/metric/query", metricController.Query)
	groupV1.GET("/metric/query_range", metricController.QueryRange)
}
