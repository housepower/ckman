package cron

const (
	JOB_NULL = iota
	JOB_SYNC_LOGIC_SCHEMA
	JOB_WATCH_CLUSTER_STATUS
)

const (
	SCHEDULE_EVERY_DAY  = "0 0 0 * * ?"
	SCHEDULE_EVERY_HOUR = "0 0 * * * ?"
	SCHEDULE_EVERY_MIN  = "0 * * * * ?"
	SCHEDULE_EVERY_SEC  = "* * * * * ?"

	SCHEDULE_WATCH_DEFAULT = "0 */3 * * * ?"
)
