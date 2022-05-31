package cron

const (
	JOB_NULL = iota
	JOB_SYNC_LOGIC_SCHEMA
)

const (
	SCHEDULE_EVERY_DAY  = "0 0 0 * * ?"
	SCHEDULE_EVERY_HOUR = "0 0 * * * ?"
	SCHEDULE_EVERY_MIN  = "0 * * * * ?"
	SCHEDULE_EVERY_SEC  = "* * * * * ?"
)
