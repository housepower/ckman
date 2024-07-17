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

type ResponseBody struct {
	RetCode string      `json:"retCode"`
	RetMsg  string      `json:"retMsg"`
	Entity  interface{} `json:"entity"`
}

func WrapMsg(c *gin.Context, retCode string, entity interface{}) {
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

	resp := ResponseBody{
		RetCode: retCode,
		RetMsg:  retMsg,
		Entity:  entity,
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

func InitRouterV1(groupV1 *gin.RouterGroup, config *config.CKManConfig, signal chan os.Signal) {
	ckController := controller.NewClickHouseController(WrapMsg)
	packageController := controller.NewPackageController(config, WrapMsg)
	deployController := controller.NewDeployController(config, WrapMsg)
	metricController := controller.NewMetricController(config, WrapMsg)
	configController := controller.NewConfigController(signal, WrapMsg)
	zkController := controller.NewZookeeperController(WrapMsg)
	uiController := controller.NewSchemaUIController(WrapMsg)
	uiController.RegistSchemaInstance()
	taskController := controller.NewTaskController(WrapMsg)

	groupV1.POST("/ck/cluster", ckController.ImportCluster)
	groupV1.GET("/ck/cluster", ckController.GetClusters)
	groupV1.GET(fmt.Sprintf("/ck/cluster/:%s", controller.ClickHouseClusterPath), ckController.GetCluster)
	groupV1.DELETE(fmt.Sprintf("/ck/cluster/:%s", controller.ClickHouseClusterPath), ckController.DeleteCluster)
	groupV1.POST(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.CreateTable)
	groupV1.POST(fmt.Sprintf("/ck/dist_logic_table/:%s", controller.ClickHouseClusterPath), ckController.CreateDistTableOnLogic)
	groupV1.DELETE(fmt.Sprintf("/ck/dist_logic_table/:%s", controller.ClickHouseClusterPath), ckController.DeleteDistTableOnLogic)
	groupV1.PUT(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.AlterTable)
	groupV1.POST(fmt.Sprintf("/ck/table/dml/:%s", controller.ClickHouseClusterPath), ckController.DMLOnLogic)
	groupV1.DELETE(fmt.Sprintf("/ck/truncate_table/:%s", controller.ClickHouseClusterPath), ckController.TruncateTable)
	groupV1.PUT(fmt.Sprintf("/ck/table/ttl/:%s", controller.ClickHouseClusterPath), ckController.AlterTableTTL)
	groupV1.PUT(fmt.Sprintf("/ck/table/readonly/:%s", controller.ClickHouseClusterPath), ckController.RestoreReplica)
	groupV1.PUT(fmt.Sprintf("/ck/table/orderby/:%s", controller.ClickHouseClusterPath), ckController.SetOrderby)
	groupV1.PUT(fmt.Sprintf("/ck/table/view/:%s", controller.ClickHouseClusterPath), ckController.MaterializedView)
	groupV1.POST(fmt.Sprintf("/ck/table/group_uniq_array/:%s", controller.ClickHouseClusterPath), ckController.GroupUniqArray)
	groupV1.GET(fmt.Sprintf("/ck/table/group_uniq_array/:%s", controller.ClickHouseClusterPath), ckController.GetGroupUniqArray)
	groupV1.DELETE(fmt.Sprintf("/ck/table/group_uniq_array/:%s", controller.ClickHouseClusterPath), ckController.DelGroupUniqArray)
	groupV1.DELETE(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DeleteTable)
	groupV1.DELETE(fmt.Sprintf("/ck/table_all/:%s", controller.ClickHouseClusterPath), ckController.DeleteTableAll)
	groupV1.GET(fmt.Sprintf("/ck/table/:%s", controller.ClickHouseClusterPath), ckController.DescTable)
	groupV1.GET(fmt.Sprintf("/ck/query/:%s", controller.ClickHouseClusterPath), ckController.QueryInfo)
	groupV1.GET(fmt.Sprintf("/ck/query_explain/:%s", controller.ClickHouseClusterPath), ckController.QueryExplain)
	groupV1.GET(fmt.Sprintf("/ck/query_export/:%s", controller.ClickHouseClusterPath), ckController.QueryExport)
	groupV1.GET(fmt.Sprintf("/ck/query_history/:%s", controller.ClickHouseClusterPath), ckController.QueryHistory)
	groupV1.DELETE(fmt.Sprintf("/ck/query_history/:%s", controller.ClickHouseClusterPath), ckController.DeleteQuery)
	groupV1.GET(fmt.Sprintf("/ck/table_lists/:%s", controller.ClickHouseClusterPath), ckController.GetTableLists)
	groupV1.GET(fmt.Sprintf("/ck/table_schema/:%s", controller.ClickHouseClusterPath), ckController.ShowSchema)
	groupV1.PUT(fmt.Sprintf("/ck/upgrade/:%s", controller.ClickHouseClusterPath), ckController.UpgradeCluster)
	groupV1.PUT(fmt.Sprintf("/ck/start/:%s", controller.ClickHouseClusterPath), ckController.StartCluster)
	groupV1.PUT(fmt.Sprintf("/ck/stop/:%s", controller.ClickHouseClusterPath), ckController.StopCluster)
	groupV1.PUT(fmt.Sprintf("/ck/destroy/:%s", controller.ClickHouseClusterPath), ckController.DestroyCluster)
	groupV1.PUT(fmt.Sprintf("/ck/rebalance/:%s", controller.ClickHouseClusterPath), ckController.RebalanceCluster)
	groupV1.GET(fmt.Sprintf("/ck/get/:%s", controller.ClickHouseClusterPath), ckController.GetClusterStatus)
	groupV1.GET(fmt.Sprintf("/ck/table_metric/:%s", controller.ClickHouseClusterPath), ckController.GetTableMetric)
	groupV1.GET(fmt.Sprintf("/ck/table_merges/:%s", controller.ClickHouseClusterPath), ckController.GetTableMerges)
	groupV1.GET(fmt.Sprintf("/ck/open_sessions/:%s", controller.ClickHouseClusterPath), ckController.GetOpenSessions)
	groupV1.PUT(fmt.Sprintf("/ck/open_sessions/:%s", controller.ClickHouseClusterPath), ckController.KillOpenSessions)
	groupV1.GET(fmt.Sprintf("/ck/slow_sessions/:%s", controller.ClickHouseClusterPath), ckController.GetSlowSessions)
	groupV1.GET(fmt.Sprintf("/ck/ddl_queue/:%s", controller.ClickHouseClusterPath), ckController.GetDistDDLQueue)
	groupV1.POST(fmt.Sprintf("/ck/node/:%s", controller.ClickHouseClusterPath), ckController.AddNode)
	groupV1.DELETE(fmt.Sprintf("/ck/node/:%s", controller.ClickHouseClusterPath), ckController.DeleteNode)
	groupV1.PUT(fmt.Sprintf("/ck/node/start/:%s", controller.ClickHouseClusterPath), ckController.StartNode)
	groupV1.PUT(fmt.Sprintf("/ck/node/stop/:%s", controller.ClickHouseClusterPath), ckController.StopNode)
	groupV1.POST(fmt.Sprintf("/ck/node/log/:%s", controller.ClickHouseClusterPath), ckController.GetLog)
	groupV1.POST(fmt.Sprintf("/ck/ping/:%s", controller.ClickHouseClusterPath), ckController.PingCluster)
	groupV1.POST(fmt.Sprintf("/ck/purge_tables/:%s", controller.ClickHouseClusterPath), ckController.PurgeTables)
	groupV1.GET(fmt.Sprintf("/ck/partition/:%s", controller.ClickHouseClusterPath), ckController.GetPartitions)
	groupV1.POST(fmt.Sprintf("/ck/archive/:%s", controller.ClickHouseClusterPath), ckController.ArchiveTable)
	groupV1.GET(fmt.Sprintf("/ck/config/:%s", controller.ClickHouseClusterPath), ckController.GetConfig)
	groupV1.POST(fmt.Sprintf("/ck/config/:%s", controller.ClickHouseClusterPath), ckController.ClusterSetting)
	groupV1.GET(fmt.Sprintf("/zk/status/:%s", controller.ClickHouseClusterPath), zkController.GetStatus)
	groupV1.GET(fmt.Sprintf("/zk/replicated_table/:%s", controller.ClickHouseClusterPath), zkController.GetReplicatedTableStatus)
	groupV1.POST("/package", packageController.Upload)
	groupV1.GET("/package", packageController.List)
	groupV1.DELETE("/package", packageController.Delete)
	groupV1.POST("/deploy/ck", deployController.DeployCk)
	groupV1.GET(fmt.Sprintf("/metric/query/:%s", controller.ClickHouseClusterPath), metricController.Query)
	groupV1.GET(fmt.Sprintf("/metric/query_range/:%s", controller.ClickHouseClusterPath), metricController.QueryRange)
	groupV1.GET("/version", configController.GetVersion)
	groupV1.GET("/ui/schema", uiController.GetUISchema)
	groupV1.GET(fmt.Sprintf("/task/:%s", controller.TaskIdPath), taskController.GetTaskStatusById)
	groupV1.GET("/task/lists", taskController.TasksList)
	groupV1.GET("/task/running", taskController.GetRunningTaskCount)
	groupV1.DELETE(fmt.Sprintf("/task/:%s", controller.TaskIdPath), taskController.DeleteTask)
	groupV1.PUT(fmt.Sprintf("/task/:%s", controller.TaskIdPath), taskController.StopTask)
}
