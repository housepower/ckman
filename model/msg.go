package model

var MsgFlags = map[int]string{
	SUCCESS:              "ok",
	ERROR:                "fail",
	INVALID_PARAMS:       "请求参数错误",
	CREAT_CK_TABLE_FAIL:  "创建ClickHouse表失败",
	DELETE_CK_TABLE_FAIL: "删除ClickHouse表失败",
	ALTER_CK_TABLE_FAIL:  "更改ClickHouse表失败",

	UNKNOWN: "unknown",
}

func GetMsg(code int) string {
	msg, ok := MsgFlags[code]
	if ok {
		return msg
	}

	return MsgFlags[UNKNOWN]
}
