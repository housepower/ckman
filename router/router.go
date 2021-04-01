package router

import (
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/controller"
	"github.com/housepower/ckman/service/nacos"
	"github.com/housepower/ckman/service/prometheus"
)

func InitRouterV1(groupV1 *gin.RouterGroup, config *config.CKManConfig, prom *prometheus.PrometheusService,
	signal chan os.Signal, nacosClient *nacos.NacosClient) {
	ckController := controller.NewClickHouseController(nacosClient)
	packageController := controller.NewPackageController(config)
	deployController := controller.NewDeployController(config, nacosClient)
	metricController := controller.NewMetricController(config, prom)
	configController := controller.NewConfigController(signal)
	zkController := controller.NewZookeeperController()

	groupV1.POST("/ck/cluster", ckController.ImportCk)
	groupV1.GET("/ck/cluster", ckController.GetCk)
	groupV1.DELETE(fmt.Sprintf("/ck/cluster/:%s", controller.ClickHouseClusterPath), ckController.DeleteCk)
	groupV1.POST(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.CreateTable)
	groupV1.PUT(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.AlterTable)
	groupV1.DELETE(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DeleteTable)
	groupV1.GET(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DescTable)
	groupV1.GET(fmt.Sprintf("/ck/query/:%s", controller.ClickHouseClusterPath), ckController.QueryInfo)
	groupV1.PUT(fmt.Sprintf("/ck/upgrade/:%s", controller.ClickHouseClusterPath), ckController.UpgradeCk)
	groupV1.PUT(fmt.Sprintf("/ck/start/:%s", controller.ClickHouseClusterPath), ckController.StartCk)
	groupV1.PUT(fmt.Sprintf("/ck/stop/:%s", controller.ClickHouseClusterPath), ckController.StopCk)
	groupV1.PUT(fmt.Sprintf("/ck/destroy/:%s", controller.ClickHouseClusterPath), ckController.DestroyCk)
	groupV1.PUT(fmt.Sprintf("/ck/rebalance/:%s", controller.ClickHouseClusterPath), ckController.RebalanceCk)
	groupV1.GET(fmt.Sprintf("/ck/get/:%s", controller.ClickHouseClusterPath), ckController.GetCkCluster)
	groupV1.GET(fmt.Sprintf("/ck/table_metric/:%s", controller.ClickHouseClusterPath), ckController.GetTableMetric)
	groupV1.GET(fmt.Sprintf("/ck/open_sessions/:%s", controller.ClickHouseClusterPath), ckController.GetOpenSessions)
	groupV1.GET(fmt.Sprintf("/ck/slow_sessions/:%s", controller.ClickHouseClusterPath), ckController.GetSlowSessions)
	groupV1.POST(fmt.Sprintf("/ck/node/:%s", controller.ClickHouseClusterPath), ckController.AddNode)
	groupV1.DELETE(fmt.Sprintf("/ck/node/:%s", controller.ClickHouseClusterPath), ckController.DeleteNode)
	groupV1.POST(fmt.Sprintf("/ck/ping/:%s", controller.ClickHouseClusterPath), ckController.PingCluster)
	groupV1.GET(fmt.Sprintf("/zk/status/:%s", controller.ClickHouseClusterPath), zkController.GetStatus)
	groupV1.GET(fmt.Sprintf("/zk/replicated_table/:%s", controller.ClickHouseClusterPath), zkController.GetReplicatedTableStatus)
	groupV1.POST("/package", packageController.Upload)
	groupV1.GET("/package", packageController.List)
	groupV1.DELETE("/package", packageController.Delete)
	groupV1.POST("/deploy/ck", deployController.DeployCk)
	groupV1.GET("/metric/query", metricController.Query)
	groupV1.GET("/metric/query_range", metricController.QueryRange)
	groupV1.PUT("/config", configController.UpdateConfig)
	groupV1.GET("/config", configController.GetConfig)
}
