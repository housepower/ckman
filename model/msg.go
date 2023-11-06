package model

import (
	"strings"

	"github.com/gin-gonic/gin"
)

type CodeMessage struct {
	Msg_EN string
	Msg_ZH string
}

var Messages = map[string]CodeMessage{
	E_SUCCESS: {"E_SUCCESS", "成功"},
	E_UNKNOWN: {"E_UNKNOWN", "未知错误"},

	E_INVALID_PARAMS:    {"E_INVALID_PARAMS", "参数不合法"},
	E_INVALID_VARIABLE:  {"E_INVALID_VARIABLE", "变量不合法"},
	E_DATA_MISMATCHED:   {"E_DATA_MISMATCHED", "数据不匹配"},
	E_DATA_CHECK_FAILED: {"E_DATA_CHECK_FAILED", "数据校验失败"},
	E_DATA_NOT_EXIST:    {"E_DATA_NOT_EXIST", "数据不存在"},
	E_DATA_DUPLICATED:   {"E_DATA_DPULICATED", "数据重复"},
	E_DATA_EMPTY:        {"E_DATA_EMPTY", "数据不允许为空"},

	E_JWT_TOKEN_EXPIRED:      {"E_JWT_TOKEN_EXPIRED", "token已过期"},
	E_JWT_TOKEN_INVALID:      {"E_JWT_TOKEN_INVALID", "token不合法"},
	E_JWT_TOKEN_NONE:         {"E_JWT_TOKEN_NONE", "token为空"},
	E_JWT_TOKEN_IP_MISMATCH:  {"E_JWT_TOKEN_IP_MISMATCH", "IP不匹配"},
	E_CREAT_TOKEN_FAIL:       {"E_CREAT_TOKEN_FAIL", "创建token失败"},
	E_USER_VERIFY_FAIL:       {"E_USER_VERIFY_FAIL", "用户校验失败"},
	E_GET_USER_PASSWORD_FAIL: {"E_GET_USER_PASSWORD_FAIL", "获取校验失败"},
	E_PASSWORD_VERIFY_FAIL:   {"E_PASSWORD_VERIFY_FAIL", "密码校验失败"},

	E_SSH_CONNECT_FAILED: {"E_SSH_CONNECT_FAILED", "SSH连接失败"},
	E_SSH_EXECUTE_FAILED: {"E_SSH_EXECUTE_FAILED", "SSH执行远程命令失败"},
	E_CH_CONNECT_FAILED:  {"E_CH_CONNECT_FAILED", "ClickHouse连接失败"},
	E_ZOOKEEPER_ERROR:    {"E_ZOOKEEPER_ERROR", "zookeeper连接失败"},

	E_CONFIG_FAILED: {"E_CONFIG_FAILED", "生成配置失败"},

	E_MARSHAL_FAILED:   {"E_MARSHAL_FAILED", "序列化失败"},
	E_UNMARSHAL_FAILED: {"E_UNMARSHAL_FAILED", "反序列化失败"},

	E_RECORD_NOT_FOUND:            {"E_RECORD_NOT_FOUND", "记录找不到"},
	E_DATA_INSERT_FAILED:          {"E_DATA_INSERT_FAILED", "数据插入失败"},
	E_DATA_UPDATE_FAILED:          {"E_DATA_UPDATE_FAILED", "数据更新失败"},
	E_DATA_DELETE_FAILED:          {"E_DATA_DELETE_FAILED", "数据删除失败"},
	E_DATA_SELECT_FAILED:          {"E_DATA_SELECT_FAILED", "数据查询失败"},
	E_TRANSACTION_DEGIN_FAILED:    {"E_TRANSACTION_DEGIN_FAILED", "事务开始失败"},
	E_TRANSACTION_COMMIT_FAILED:   {"E_TRANSACTION_COMMIT_FAILED", "事务提交失败"},
	E_TRANSACTION_ROLLBACK_FAILED: {"E_TRANSACTION_ROLLBACK_FAILED", "事务回滚失败"},
	E_TBL_CREATE_FAILED:           {"E_TBL__CREATE_FAILED", "创建表失败"},
	E_TBL_ALTER_FAILED:            {"E_TBL_ALTER_FAILED", "修改表失败"},
	E_TBL_DROP_FAILED:             {"E_TBL_DROP_FAILED", "删除表失败"},
	E_TBL_EXISTS:                  {"E_TBL_EXISTS", "表已存在"},
	E_TBL_NOT_EXISTS:              {"E_TBL_NOT_EXISTS", "表不存在"},
	E_TBL_BACKUP_FAILED:           {"E_TBL_BACKUP_FAILED", "备份表失败"},
	E_TBL_RESTORE_FAILED:          {"E_TBL_RESTORE_FAILED", "还原表失败"},
}

func GetMsg(c *gin.Context, code string) string {
	lang := c.Request.Header.Get("Accept-Language")
	var msg string
	if strings.Contains(lang, "zh") {
		msg = Messages[code].Msg_ZH
	} else {
		msg = Messages[code].Msg_EN
	}
	return msg
}
