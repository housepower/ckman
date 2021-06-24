package model

const (
	SUCCESS                     = 0
	INVALID_PARAMS              = 5000
	CREAT_CK_TABLE_FAIL         = 5001
	DELETE_CK_TABLE_FAIL        = 5002
	ALTER_CK_TABLE_FAIL         = 5003
	UPLOAD_LOCAL_PACKAGE_FAIL   = 5004
	UPLOAD_PEER_PACKAGE_FAIL    = 5005
	DELETE_LOCAL_PACKAGE_FAIL   = 5006
	DELETE_PEER_PACKAGE_FAIL    = 5007
	LIST_PACKAGE_FAIL           = 5008
	INIT_PACKAGE_FAIL           = 5011
	PREPARE_PACKAGE_FAIL        = 5012
	INSTALL_PACKAGE_FAIL        = 5013
	CONFIG_PACKAGE_FAIL         = 5014
	START_PACKAGE_FAIL          = 5015
	CHECK_PACKAGE_FAIL          = 5016
	JWT_TOKEN_EXPIRED           = 5020
	JWT_TOKEN_INVALID           = 5021
	JWT_TOKEN_NONE              = 5022
	JWT_TOKEN_IP_MISMATCH       = 5023
	USER_VERIFY_FAIL            = 5030
	GET_USER_PASSWORD_FAIL      = 5031
	PASSWORD_VERIFY_FAIL        = 5032
	CREAT_TOKEN_FAIL            = 5033
	DESC_CK_TABLE_FAIL          = 5040
	CONNECT_CK_CLUSTER_FAIL     = 5041
	IMPORT_CK_CLUSTER_FAIL      = 5042
	UPDATE_CK_CLUSTER_FAIL      = 5043
	QUERY_CK_FAIL               = 5044
	QUERY_METRIC_FAIL           = 5050
	QUERY_RANGE_METRIC_FAIL     = 5051
	UPGRADE_CK_CLUSTER_FAIL     = 5060
	START_CK_CLUSTER_FAIL       = 5061
	STOP_CK_CLUSTER_FAIL        = 5062
	DESTROY_CK_CLUSTER_FAIL     = 5063
	REBALANCE_CK_CLUSTER_FAIL   = 5064
	GET_CK_CLUSTER_INFO_FAIL    = 5065
	ADD_CK_CLUSTER_NODE_FAIL    = 5066
	DELETE_CK_CLUSTER_NODE_FAIL = 5067
	GET_CK_TABLE_METRIC_FAIL    = 5068
	UPDATE_CONFIG_FAIL          = 5070
	GET_ZK_STATUS_FAIL          = 5080
	GET_ZK_TABLE_STATUS_FAIL    = 5081
	GET_CK_OPEN_SESSIONS_FAIL   = 5090
	GET_CK_SLOW_SESSIONS_FAIL   = 5091
	GET_NACOS_CONFIG_FAIL       = 5100
	PUB_NACOS_CONFIG_FAIL       = 5101
	DEPLOY_USER_RETAIN_ERROR    = 5200
	PING_CK_CLUSTER_FAIL        = 5201
	CLUSTER_NOT_EXIST           = 5202
	PURGER_TABLES_FAIL          = 5203
	ARCHIVE_TO_HDFS_FAIL        = 5204
	SHOW_SCHEMER_ERROR          = 5205

	UNKNOWN = 99999
)
