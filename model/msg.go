package model

var MsgFlags = map[int]string{
	SUCCESS:                     "ok",
	ERROR:                       "fail",
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
	QUERY_RANGE_METRIC_FAIL:     "获取指标范围失败",
	QUERY_CK_FAIL:               "查询ClickHouse失败",
	CONNECT_CK_CLUSTER_FAIL:     "连接ClickHouse集群失败",
	IMPORT_CK_CLUSTER_FAIL:      "导入ClickHouse集群失败",
	UPDATE_CK_CLUSTER_FAIL:      "更新ClickHouse集群失败",
	UPGRADE_CK_CLUSTER_FAIL:     "升级ClickHouse集群失败",
	START_CK_CLUSTER_FAIL:       "启动ClickHouse服务失败",
	STOP_CK_CLUSTER_FAIL:        "停止ClickHouse集群失败",
	DESTROY_CK_CLUSTER_FAIL:     "销毁ClickHouse集群失败",
	REBALANCE_CK_CLUSTER_FAIL:   "均衡ClickHouse集群失败",
	GET_CK_CLUSTER_INFO_FAIL:    "获取ClickHouse集群信息失败",
	ADD_CK_CLUSTER_NODE_FAIL:    "添加ClickHouse集群节点失败",
	DELETE_CK_CLUSTER_NODE_FAIL: "删除ClickHouse集群节点失败",
	UPDATE_CONFIG_FAIL:          "更新配置失败",
	GET_ZK_STATUS_FAIL:          "获取Zookeeper状态失败",

	UNKNOWN: "unknown",
}

func GetMsg(code int) string {
	msg, ok := MsgFlags[code]
	if ok {
		return msg
	}

	return MsgFlags[UNKNOWN]
}
