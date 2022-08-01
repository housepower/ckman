package cron

const (
	JOB_NULL = iota
	JOB_SYNC_LOGIC_SCHEMA
	JOB_WATCH_CLUSTER_STATUS
	JOB_SYNC_DIST_SCHEMA
)

const (
	SCHEDULE_EVERY_DAY  = "0 0 0 * * ?"
	SCHEDULE_EVERY_HOUR = "0 0 * * * ?"
	SCHEDULE_EVERY_MIN  = "0 * * * * ?"
	SCHEDULE_EVERY_SEC  = "* * * * * ?"

	SCHEDULE_WATCH_DEFAULT = "0 */3 * * * ?"
	SCHEDULE_SYNC_DIST     = "30 */10 * * * ?"
)
