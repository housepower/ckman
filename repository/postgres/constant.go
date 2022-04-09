package postgres

const (
	PostgresPersistentName    string = "postgres"
	PG_PORT_DEFAULT           int    = 5432
	PG_DATABASE_DEFAULT       string = "ckman_db"
	PG_MAX_IDLE_CONNS_DEFAULT int    = 10
	PG_MAX_OPEN_CONNS_DEFAULT int    = 100
	PG_MAX_LIFETIME_DEFAULT   int    = 3600
	PG_MAX_IDLE_TIME_DEFAULT  int    = 10

	PG_TBL_CLUSTER       string = "tbl_cluster"
	PG_TBL_LOGIC         string = "tbl_logic"
	PG_TBL_QUERY_HISTORY string = "tbl_query_history"
	PG_TBL_TASK          string = "tbl_task"
)
