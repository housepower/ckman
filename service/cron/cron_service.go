package cron

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/robfig/cron/v3"
)

type CronService struct {
	config       config.CronJob
	jobSchedules map[int16]string
	cron         *cron.Cron
}

var JobList = map[int16]func() error{
	JOB_SYNC_LOGIC_SCHEMA:    SyncLogicSchema,
	JOB_WATCH_CLUSTER_STATUS: WatchClusterStatus,
	JOB_SYNC_DIST_SCHEMA:     SyncDistSchema,
	JOB_CLEAR_ZNODES:         ClearZnodes,
}

func NewCronService(config config.CronJob) *CronService {
	return &CronService{
		config:       config,
		jobSchedules: make(map[int16]string),
		cron:         cron.New(cron.WithSeconds()),
	}
}

func (job *CronService) schedulePadding() {
	job.jobSchedules[JOB_SYNC_LOGIC_SCHEMA] = common.GetStringwithDefault(job.config.SyncLogicSchema, SCHEDULE_EVERY_MIN)
	job.jobSchedules[JOB_WATCH_CLUSTER_STATUS] = common.GetStringwithDefault(job.config.WatchClusterStatus, SCHEDULE_WATCH_DEFAULT)
	job.jobSchedules[JOB_SYNC_DIST_SCHEMA] = common.GetStringwithDefault(job.config.SyncDistSchema, SCHEDULE_SYNC_DIST)
	job.jobSchedules[JOB_CLEAR_ZNODES] = common.GetStringwithDefault(job.config.ClearZnodes, SCHEDULE_CLEAR_ZNODES)
}

func (job *CronService) Start() error {
	//Ensure that only one node in the same cluster executes scheduled tasks
	if !config.IsMasterNode() {
		log.Logger.Debugf("node %s:%d is not master, skip all cron jobs", config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)
		return nil
	}
	if !job.config.Enabled {
		return nil
	}
	job.schedulePadding()
	job.cron.Start()
	for k, v := range JobList {
		k := k
		v := v
		if spec, ok := job.jobSchedules[k]; ok {
			if spec == SCHEDULE_DISABLED {
				continue
			}
			_, _ = job.cron.AddFunc(spec, func() {
				_ = v()
			})
		}
	}
	return nil
}

func (job *CronService) Stop() {
	job.cron.Stop()
	log.Logger.Infof("cron service stopped")
}
