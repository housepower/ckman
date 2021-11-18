package model

import (
	"github.com/gin-gonic/gin"
	"strings"
)

var MsgFlags_zh = map[string]string{
	SUCCESS:                     "ok",
	INVALID_PARAMS:              "请求参数错误",
	CREAT_CK_TABLE_FAIL:         "创建ClickHouse表失败",
	DELETE_CK_TABLE_FAIL:        "删除ClickHouse表失败",
	ALTER_CK_TABLE_FAIL:         "更改ClickHouse表失败",
	UPLOAD_LOCAL_PACKAGE_FAIL:   "上传安装包到本地失败",
	UPLOAD_PEER_PACKAGE_FAIL:    "上传安装包到邻近节点失败",
	DELETE_LOCAL_PACKAGE_FAIL:   "删除本地安装包失败",
	DELETE_PEER_PACKAGE_FAIL:    "删除邻近节点安装包失败",
	LIST_PACKAGE_FAIL:           "获取安装包列表失败",
	INIT_PACKAGE_FAIL:           "初始化组件失败",
	PREPARE_PACKAGE_FAIL:        "准备组件失败",
	INSTALL_PACKAGE_FAIL:        "安装组件失败",
	CONFIG_PACKAGE_FAIL:         "配置组件失败",
	START_PACKAGE_FAIL:          "启动组件失败",
	CHECK_PACKAGE_FAIL:          "检查组件启动状态失败",
	CONFIG_CLUSTER_FAIL:         "修改集群配置失败",
	JWT_TOKEN_EXPIRED:           "token已过期",
	JWT_TOKEN_INVALID:           "无效的token",
	JWT_TOKEN_NONE:              "请求未携带token",
	JWT_TOKEN_IP_MISMATCH:       "Ip不匹配",
	USER_VERIFY_FAIL:            "该用户不存在",
	GET_USER_PASSWORD_FAIL:      "获取用户密码失败",
	PASSWORD_VERIFY_FAIL:        "用户密码验证失败",
	CREAT_TOKEN_FAIL:            "生成token失败",
	DESC_CK_TABLE_FAIL:          "描述ClickHouse表失败",
	QUERY_METRIC_FAIL:           "获取指标失败",
	DELETE_CK_CLUSTER_FAIL:      "删除集群失败",
	QUERY_RANGE_METRIC_FAIL:     "获取指标范围失败",
	QUERY_CK_FAIL:               "查询ClickHouse失败",
	CONNECT_CK_CLUSTER_FAIL:     "连接ClickHouse集群失败",
	IMPORT_CK_CLUSTER_FAIL:      "导入ClickHouse集群失败",
	UPDATE_CK_CLUSTER_FAIL:      "更新ClickHouse集群失败",
	START_CK_NODE_FAIL:          "节点上线失败",
	STOP_CK_NODE_FAIL:           "节点下线失败",
	UPGRADE_CK_CLUSTER_FAIL:     "升级ClickHouse集群失败",
	START_CK_CLUSTER_FAIL:       "启动ClickHouse服务失败",
	STOP_CK_CLUSTER_FAIL:        "停止ClickHouse集群失败",
	DESTROY_CK_CLUSTER_FAIL:     "销毁ClickHouse集群失败",
	REBALANCE_CK_CLUSTER_FAIL:   "均衡ClickHouse集群失败",
	GET_CK_CLUSTER_INFO_FAIL:    "获取ClickHouse集群信息失败",
	ADD_CK_CLUSTER_NODE_FAIL:    "添加ClickHouse集群节点失败",
	DELETE_CK_CLUSTER_NODE_FAIL: "删除ClickHouse集群节点失败",
	GET_CK_TABLE_METRIC_FAIL:    "获取ClickHouse表的指标失败",
	UPDATE_CONFIG_FAIL:          "更新配置失败",
	GET_ZK_STATUS_FAIL:          "获取Zookeeper状态失败",
	GET_ZK_TABLE_STATUS_FAIL:    "获取复制表状态失败",
	GET_TABLE_LISTS_FAILED:      "获取数据库表信息失败",
	GET_CK_OPEN_SESSIONS_FAIL:   "获取ClickHouse进行中的查询失败",
	GET_CK_SLOW_SESSIONS_FAIL:   "获取ClickHouse慢查询失败",
	GET_TASK_FAIL:               "获取任务失败",
	DELETE_TASK_FAIL:            "删除任务失败",
	DEPLOY_CK_CLUSTER_ERROR:     "部署集群失败",
	PING_CK_CLUSTER_FAIL:        "ClickHouse集群节点无法连接",
	CLUSTER_NOT_EXIST:           "集群不存在",
	PURGER_TABLES_FAIL:          "删除指定时间范围内数据失败",
	ARCHIVE_TO_HDFS_FAIL:        "归档到HDFS失败",
	SHOW_SCHEMA_ERROR:           "查看建表语句失败",
	GET_SCHEMA_UI_FAILED:        "获取前端schema失败",

	UNKNOWN: "unknown",
}

var MsgFlags_en = map[string]string{
	SUCCESS:                     "ok",
	INVALID_PARAMS:              "invalid params",
	CREAT_CK_TABLE_FAIL:         "create ClickHouse table failed",
	DELETE_CK_TABLE_FAIL:        "delete ClickHouse table failed",
	ALTER_CK_TABLE_FAIL:         "alter ClickHouse table failed",
	UPLOAD_LOCAL_PACKAGE_FAIL:   "upload local package failed",
	UPLOAD_PEER_PACKAGE_FAIL:    "upload peer package failed",
	DELETE_LOCAL_PACKAGE_FAIL:   "delete local package failed",
	DELETE_PEER_PACKAGE_FAIL:    "delete peer package failed",
	LIST_PACKAGE_FAIL:           "get package list failed",
	INIT_PACKAGE_FAIL:           "init package failed",
	PREPARE_PACKAGE_FAIL:        "prepare package failed",
	INSTALL_PACKAGE_FAIL:        "install package failed",
	CONFIG_PACKAGE_FAIL:         "config package failed",
	START_PACKAGE_FAIL:          "start package fialed",
	CHECK_PACKAGE_FAIL:          "check package failed",
	CONFIG_CLUSTER_FAIL:         "config cluster failed",
	JWT_TOKEN_EXPIRED:           "token has expired",
	JWT_TOKEN_INVALID:           "invalid token",
	JWT_TOKEN_NONE:              "request did not carry a token",
	JWT_TOKEN_IP_MISMATCH:       "Ip mismatched",
	USER_VERIFY_FAIL:            "user verify failed",
	GET_USER_PASSWORD_FAIL:      "get user and password failed",
	PASSWORD_VERIFY_FAIL:        "password verify failed",
	CREAT_TOKEN_FAIL:            "create token failed",
	DESC_CK_TABLE_FAIL:          "describe ClickHouse table failed",
	QUERY_METRIC_FAIL:           "get query metric failed",
	DELETE_CK_CLUSTER_FAIL:      "delete cluster failed",
	QUERY_RANGE_METRIC_FAIL:     "get range-metric failed",
	QUERY_CK_FAIL:               "query ClickHouse failed",
	CONNECT_CK_CLUSTER_FAIL:     "connect ClickHouse cluster failed",
	IMPORT_CK_CLUSTER_FAIL:      "import ClickHouse cluster failed",
	START_CK_NODE_FAIL:          "start node failed",
	STOP_CK_NODE_FAIL:           "stop node failed",
	UPDATE_CK_CLUSTER_FAIL:      "update ClickHouse cluster failed",
	UPGRADE_CK_CLUSTER_FAIL:     "upgrade ClickHouse cluster failed",
	START_CK_CLUSTER_FAIL:       "start ClickHouse cluster failed",
	STOP_CK_CLUSTER_FAIL:        "stop ClickHouse cluster failed",
	DESTROY_CK_CLUSTER_FAIL:     "destroy ClickHouse cluster failed",
	REBALANCE_CK_CLUSTER_FAIL:   "rebalance ClickHouse cluster failed",
	GET_CK_CLUSTER_INFO_FAIL:    "get ClickHouse cluster information failed",
	ADD_CK_CLUSTER_NODE_FAIL:    "add ClickHouse node failed",
	DELETE_CK_CLUSTER_NODE_FAIL: "delete ClickHouse node failed",
	GET_CK_TABLE_METRIC_FAIL:    "get metric of ClickHouse table failed",
	UPDATE_CONFIG_FAIL:          "update config failed",
	GET_ZK_STATUS_FAIL:          "get Zookeeper status failed",
	GET_ZK_TABLE_STATUS_FAIL:    "get Zookeeper table status failed",
	GET_TABLE_LISTS_FAILED:      "get table lists failed",
	GET_CK_OPEN_SESSIONS_FAIL:   "get open sessions failed",
	GET_CK_SLOW_SESSIONS_FAIL:   "get slow sessions failed",
	GET_TASK_FAIL:               "get task infomation failed",
	DELETE_TASK_FAIL:            "delete task failed",
	DEPLOY_CK_CLUSTER_ERROR:     "deploy cluster failed",
	PING_CK_CLUSTER_FAIL:        "ClickHouse cluster can't ping all nodes successfully",
	CLUSTER_NOT_EXIST:           "Cluster does not exist",
	PURGER_TABLES_FAIL:          "purger tables range failed",
	ARCHIVE_TO_HDFS_FAIL:        "archive to hdfs failed",
	SHOW_SCHEMA_ERROR:           "show create table schemer failed",
	GET_SCHEMA_UI_FAILED:        "ger ui schema fialed",
	UNKNOWN:                     "unknown",
}

func GetMsg(c *gin.Context, code string) string {
	lang := c.Request.Header.Get("Accept-Language")
	var MsgFlags map[string]string
	if strings.Contains(lang, "zh") {
		MsgFlags = MsgFlags_zh
	} else {
		MsgFlags = MsgFlags_en
	}
	msg, ok := MsgFlags[code]
	if ok {
		return msg
	}

	return MsgFlags[UNKNOWN]
}
