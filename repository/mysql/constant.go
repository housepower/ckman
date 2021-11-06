package mysql

const (
	MySQLPersistentName          string = "mysql"
	MYSQL_PORT_DEFAULT           int    = 3306
	MYSQL_DATABASE_DEFAULT       string = "ckman_db"
	MYSQL_MAX_IDLE_CONNS_DEFAULT int    = 10
	MYSQL_MAX_OPEN_CONNS_DEFAULT int    = 100
	MYSQL_MAX_LIFETIME_DEFAULT   int    = 3600
	MYSQL_MAX_IDLE_TIME_DEFAULT  int    = 10

	MYSQL_TBL_CLUSTER       string = "tbl_cluster"
	MYSQL_TBL_LOGIC         string = "tbl_logic"
	MYSQL_TBL_QUERY_HISTORY string = "tbl_query_history"
	MYSQL_TBL_TASK          string = "tbl_task"
)
