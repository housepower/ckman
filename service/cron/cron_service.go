package cron

import (
	"context"
	"sync"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
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
	job.jobSchedules[JOB_CLEAR_ZNODES] = common.GetStringwithDefault(job.config.ClearZnodes, SCHEDULE_DISABLED)
}

func (job *CronService) Start() error {
	if !job.config.Enabled {
		return nil
	}
	job.schedulePadding()
	job.cron.Start()
	go job.RunDynamicJobs()
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

const (
	JOB_ADD = iota
	JOB_UPD
	JOB_DEL
)

const (
	JOB_READY = iota
	JOB_SCHEDULED
)

type Job struct {
	op      int
	spec    string
	fn      func() error
	status  int
	entryId cron.EntryID
}

var (
	lock        sync.Mutex
	DynamicJobs map[string]Job
)

func AddJob(id, spec string, fn func() error) {
	log.Logger.Infof("add job: %s, spec: %s", id, spec)
	lock.Lock()
	DynamicJobs[id] = Job{
		op:     JOB_ADD,
		spec:   spec,
		fn:     fn,
		status: JOB_READY,
	}
	lock.Unlock()
}

func RemoveJob(id string) {
	log.Logger.Infof("remove job: %s", id)
	lock.Lock()
	if j, ok := DynamicJobs[id]; ok {
		j.op = JOB_DEL
		DynamicJobs[id] = j
	}
	lock.Unlock()
}

func (job *CronService) RunDynamicJobs() {
	if DynamicJobs == nil {
		DynamicJobs = make(map[string]Job)
	}
	backups, err := repository.Ps.GetBackupByShechuleType(model.BACKUP_SCHEDULED)
	if err != nil {
		log.Logger.Errorf("get backup failed: %v", err)
		return
	}
	log.Logger.Infof("found backups: %d", len(backups))
	for _, backup := range backups {
		log.Logger.Infof("add backup job: %s, spec: %s", backup.BackupId, backup.Crontab)
		AddJob(backup.BackupId, backup.Crontab, func() error {
			return clickhouse.BackupManage(backup.BackupId)
		})
	}

	ticker := time.NewTicker(time.Second * 10)
	ctx := context.Background()
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lock.Lock()
			for k, v := range DynamicJobs {
				switch v.op {
				case JOB_ADD:
					if v.status == JOB_SCHEDULED {
						continue
					}
					log.Logger.Infof("job added: %s, spec: %s", k, v.spec)
					v.entryId, _ = job.cron.AddFunc(v.spec, func() {
						_ = v.fn()
					})
					v.status = JOB_SCHEDULED
					DynamicJobs[k] = v
				case JOB_DEL:
					job.cron.Remove(v.entryId)
					delete(DynamicJobs, k)
					log.Logger.Infof("job removed: %s", k)
				}
			}
			lock.Unlock()
		}
	}
}
