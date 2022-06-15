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
	JOB_SYNC_LOGIC_SCHEMA: SyncLogicSchema,
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
}

func (job *CronService) Start() error {
	job.schedulePadding()
	job.cron.Start()
	for k, v := range JobList {
		k := k
		v := v
		if spec, ok := job.jobSchedules[k]; ok {
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
