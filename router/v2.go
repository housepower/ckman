package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/controller"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

type ResponseBody2 struct {
	Code string      `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func WrapMsg2(c *gin.Context, retCode string, entity interface{}) {
	c.Status(http.StatusOK)
	c.Header("Content-Type", "application/json; charset=utf-8")

	retMsg := model.GetMsg(c, retCode)
	if retCode != model.E_SUCCESS {
		log.Logger.Errorf("%s %s return %s, %v", c.Request.Method, c.Request.RequestURI, retCode, entity)
		if err, ok := entity.(error); ok {
			err = common.ClikHouseExceptionDecode(err)
			var exception *clickhouse.Exception
			if errors.As(err, &exception) {
				retCode = fmt.Sprintf("%04d", exception.Code)
				retMsg += ": " + exception.Message
				log.Logger.Errorf("ClickHouse server StackTrace:\n" + exception.StackTrace)
			} else {
				retMsg += ": " + err.Error()
			}
		} else if s, ok := entity.(string); ok {
			retMsg += ": " + s
		}
		entity = nil
	}

	resp := ResponseBody2{
		Code: retCode,
		Msg:  retMsg,
		Data: entity,
	}
	jsonBytes, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Logger.Errorf("%s %s marshal response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return
	}

	log.Logger.Debugf("[response] | %s | %s | %s \n%v", c.Request.Host, c.Request.Method, c.Request.URL, string(jsonBytes))

	_, err = c.Writer.Write(jsonBytes)
	if err != nil {
		log.Logger.Errorf("%s %s write response body fail: %s", c.Request.Method, c.Request.RequestURI, err.Error())
		return
	}
}

func InitRouterV2(groupV2 *gin.RouterGroup, config *config.CKManConfig, signal chan os.Signal) {
	ckController := controller.NewClickHouseController(WrapMsg2)
	packageController := controller.NewPackageController(config, WrapMsg2)
	deployController := controller.NewDeployController(config, WrapMsg2)
	metricController := controller.NewMetricController(config, WrapMsg2)
	configController := controller.NewConfigController(signal, WrapMsg2)
	zkController := controller.NewZookeeperController(WrapMsg2)
	uiController := controller.NewSchemaUIController(WrapMsg2)
	uiController.RegistSchemaInstance()
	taskController := controller.NewTaskController(WrapMsg2)

	groupV2.POST("/ck/cluster", ckController.ImportCluster)
	groupV2.GET("/ck/cluster", ckController.GetClusters)
	groupV2.GET(fmt.Sprintf("/ck/cluster/:%s", controller.ClickHouseClusterPath), ckController.GetCluster)
	groupV2.DELETE(fmt.Sprintf("/ck/cluster/:%s", controller.ClickHouseClusterPath), ckController.DeleteCluster)
	groupV2.POST(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.CreateTable)
	groupV2.POST(fmt.Sprintf("/ck/dist-logic-table/:%s", controller.ClickHouseClusterPath), ckController.CreateDistTableOnLogic)
	groupV2.DELETE(fmt.Sprintf("/ck/dist-logic-table/:%s", controller.ClickHouseClusterPath), ckController.DeleteDistTableOnLogic)
	groupV2.PUT(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.AlterTable)
	groupV2.POST(fmt.Sprintf("/ck/table/dml/:%s", controller.ClickHouseClusterPath), ckController.DMLOnLogic)
	groupV2.DELETE(fmt.Sprintf("/ck/truncate-table/:%s", controller.ClickHouseClusterPath), ckController.TruncateTable)
	groupV2.PUT(fmt.Sprintf("/ck/table/ttl/:%s", controller.ClickHouseClusterPath), ckController.AlterTableTTL)
	groupV2.PUT(fmt.Sprintf("/ck/table/readonly/:%s", controller.ClickHouseClusterPath), ckController.RestoreReplica)
	groupV2.PUT(fmt.Sprintf("/ck/table/orderby/:%s", controller.ClickHouseClusterPath), ckController.SetOrderby)
	groupV2.PUT(fmt.Sprintf("/ck/table/view/:%s", controller.ClickHouseClusterPath), ckController.MaterializedView)
	groupV2.POST(fmt.Sprintf("/ck/table/group-uniq-array/:%s", controller.ClickHouseClusterPath), ckController.GroupUniqArray)
	groupV2.GET(fmt.Sprintf("/ck/table/group-uniq-array/:%s", controller.ClickHouseClusterPath), ckController.GetGroupUniqArray)
	groupV2.DELETE(fmt.Sprintf("/ck/table/group-uniq-array/:%s", controller.ClickHouseClusterPath), ckController.DelGroupUniqArray)
	groupV2.DELETE(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DeleteTable)
	groupV2.DELETE(fmt.Sprintf("/ck/table-all/:%s", controller.ClickHouseClusterPath), ckController.DeleteTableAll)
	groupV2.GET(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DescTable)
	groupV2.GET(fmt.Sprintf("/ck/query/:%s", controller.ClickHouseClusterPath), ckController.QueryInfo)
	groupV2.GET(fmt.Sprintf("/ck/query-explain/:%s", controller.ClickHouseClusterPath), ckController.QueryExplain)
	groupV2.GET(fmt.Sprintf("/ck/query-export/:%s", controller.ClickHouseClusterPath), ckController.QueryExport)
	groupV2.GET(fmt.Sprintf("/ck/query-history/:%s", controller.ClickHouseClusterPath), ckController.QueryHistory)
	groupV2.DELETE(fmt.Sprintf("/ck/query-history/:%s", controller.ClickHouseClusterPath), ckController.DeleteQuery)
	groupV2.GET(fmt.Sprintf("/ck/table-lists/:%s", controller.ClickHouseClusterPath), ckController.GetTableLists)
	groupV2.GET(fmt.Sprintf("/ck/table-schema/:%s", controller.ClickHouseClusterPath), ckController.ShowSchema)
	groupV2.PUT(fmt.Sprintf("/ck/upgrade/:%s", controller.ClickHouseClusterPath), ckController.UpgradeCluster)
	groupV2.PUT(fmt.Sprintf("/ck/start/:%s", controller.ClickHouseClusterPath), ckController.StartCluster)
	groupV2.PUT(fmt.Sprintf("/ck/stop/:%s", controller.ClickHouseClusterPath), ckController.StopCluster)
	groupV2.PUT(fmt.Sprintf("/ck/destroy/:%s", controller.ClickHouseClusterPath), ckController.DestroyCluster)
	groupV2.PUT(fmt.Sprintf("/ck/rebalance/:%s", controller.ClickHouseClusterPath), ckController.RebalanceCluster)
	groupV2.GET(fmt.Sprintf("/ck/get/:%s", controller.ClickHouseClusterPath), ckController.GetClusterStatus)
	groupV2.GET(fmt.Sprintf("/ck/table-metric/:%s", controller.ClickHouseClusterPath), ckController.GetTableMetric)
	groupV2.GET(fmt.Sprintf("/ck/table-merges/:%s", controller.ClickHouseClusterPath), ckController.GetTableMerges)
	groupV2.GET(fmt.Sprintf("/ck/open-sessions/:%s", controller.ClickHouseClusterPath), ckController.GetOpenSessions)
	groupV2.PUT(fmt.Sprintf("/ck/open-sessions/:%s", controller.ClickHouseClusterPath), ckController.KillOpenSessions)
	groupV2.GET(fmt.Sprintf("/ck/slow-sessions/:%s", controller.ClickHouseClusterPath), ckController.GetSlowSessions)
	groupV2.GET(fmt.Sprintf("/ck/ddl_queue/:%s", controller.ClickHouseClusterPath), ckController.GetDistDDLQueue)
	groupV2.POST(fmt.Sprintf("/ck/node/:%s", controller.ClickHouseClusterPath), ckController.AddNode)
	groupV2.DELETE(fmt.Sprintf("/ck/node/:%s", controller.ClickHouseClusterPath), ckController.DeleteNode)
	groupV2.PUT(fmt.Sprintf("/ck/node/start/:%s", controller.ClickHouseClusterPath), ckController.StartNode)
	groupV2.PUT(fmt.Sprintf("/ck/node/stop/:%s", controller.ClickHouseClusterPath), ckController.StopNode)
	groupV2.POST(fmt.Sprintf("/ck/node/log/:%s", controller.ClickHouseClusterPath), ckController.GetLog)
	groupV2.POST(fmt.Sprintf("/ck/ping/:%s", controller.ClickHouseClusterPath), ckController.PingCluster)
	groupV2.POST(fmt.Sprintf("/ck/purge-tables/:%s", controller.ClickHouseClusterPath), ckController.PurgeTables)
	groupV2.GET(fmt.Sprintf("/ck/partition/:%s", controller.ClickHouseClusterPath), ckController.GetPartitions)
	groupV2.POST(fmt.Sprintf("/ck/archive/:%s", controller.ClickHouseClusterPath), ckController.ArchiveTable)
	groupV2.GET(fmt.Sprintf("/ck/config/:%s", controller.ClickHouseClusterPath), ckController.GetConfig)
	groupV2.POST(fmt.Sprintf("/ck/config/:%s", controller.ClickHouseClusterPath), ckController.ClusterSetting)
	groupV2.GET(fmt.Sprintf("/zk/status/:%s", controller.ClickHouseClusterPath), zkController.GetStatus)
	groupV2.GET(fmt.Sprintf("/zk/replicated-table/:%s", controller.ClickHouseClusterPath), zkController.GetReplicatedTableStatus)
	groupV2.POST("/package", packageController.Upload)
	groupV2.GET("/package", packageController.List)
	groupV2.DELETE("/package", packageController.Delete)
	groupV2.POST("/deploy/ck", deployController.DeployCk)
	groupV2.GET(fmt.Sprintf("/metric/query/:%s", controller.ClickHouseClusterPath), metricController.Query)
	groupV2.GET(fmt.Sprintf("/metric/query-range/:%s", controller.ClickHouseClusterPath), metricController.QueryRange)
	groupV2.GET("/version", configController.GetVersion)
	groupV2.GET("/ui/schema", uiController.GetUISchema)
	groupV2.GET(fmt.Sprintf("/task/:%s", controller.TaskIdPath), taskController.GetTaskStatusById)
	groupV2.GET("/task/lists", taskController.TasksList)
	groupV2.GET("/task/running", taskController.GetRunningTaskCount)
	groupV2.DELETE(fmt.Sprintf("/task/:%s", controller.TaskIdPath), taskController.DeleteTask)
	groupV2.PUT(fmt.Sprintf("/task/:%s", controller.TaskIdPath), taskController.StopTask)
}
