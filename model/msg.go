package model

var MsgFlags = map[int]string{
	SUCCESS:                   "ok",
	ERROR:                     "fail",
	INVALID_PARAMS:            "请求参数错误",
	CREAT_CK_TABLE_FAIL:       "创建ClickHouse表失败",
	DELETE_CK_TABLE_FAIL:      "删除ClickHouse表失败",
	ALTER_CK_TABLE_FAIL:       "更改ClickHouse表失败",
	UPLOAD_LOCAL_PACKAGE_FAIL: "上传安装包到本地失败",
	UPLOAD_PEER_PACKAGE_FAIL:  "上传安装包到邻近节点失败",
	DELETE_LOCAL_PACKAGE_FAIL: "删除本地安装包失败",
	DELETE_PEER_PACKAGE_FAIL:  "删除邻近节点安装包失败",
	LIST_PACKAGE_FAIL:         "获取安装包列表失败",
	INIT_PACKAGE_FAIL:         "初始化组件失败",
	PREPARE_PACKAGE_FAIL:      "准备组件失败",
	INSTALL_PACKAGE_FAIL:      "安装组件失败",
	CONFIG_PACKAGE_FAIL:       "配置组件失败",
	START_PACKAGE_FAIL:        "启动组件失败",
	CHECK_PACKAGE_FAIL:        "检查组件启动状态失败",

	UNKNOWN: "unknown",
}

func GetMsg(code int) string {
	msg, ok := MsgFlags[code]
	if ok {
		return msg
	}

	return MsgFlags[UNKNOWN]
}
