package cron

import (
	"context"
	"fmt"
	"net"
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
	config       config.CKManConfig
	jobSchedules map[int16]string
	cron         *cron.Cron
}

var JobList = map[int16]func() error{
	JOB_SYNC_LOGIC_SCHEMA:    SyncLogicSchema,
	JOB_WATCH_CLUSTER_STATUS: WatchClusterStatus,
	JOB_SYNC_DIST_SCHEMA:     SyncDistSchema,
	JOB_CLEAR_ZNODES:         ClearZnodes,
}

func NewCronService(config config.CKManConfig) *CronService {
	return &CronService{
		config:       config,
		jobSchedules: make(map[int16]string),
		cron:         cron.New(cron.WithSeconds()),
	}
}

func (job *CronService) schedulePadding() {
	job.jobSchedules[JOB_SYNC_LOGIC_SCHEMA] = common.GetStringwithDefault(job.config.Cron.SyncLogicSchema, SCHEDULE_EVERY_MIN)
	job.jobSchedules[JOB_WATCH_CLUSTER_STATUS] = common.GetStringwithDefault(job.config.Cron.WatchClusterStatus, SCHEDULE_WATCH_DEFAULT)
	job.jobSchedules[JOB_SYNC_DIST_SCHEMA] = common.GetStringwithDefault(job.config.Cron.SyncDistSchema, SCHEDULE_SYNC_DIST)
	job.jobSchedules[JOB_CLEAR_ZNODES] = common.GetStringwithDefault(job.config.Cron.ClearZnodes, SCHEDULE_DISABLED)
}

func (job *CronService) Start() error {
	if !job.config.Cron.Enabled {
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
			if _, err := job.cron.AddFunc(spec, func() {
				_ = v()
			}); err != nil {
				log.Logger.Errorf("add cron job failed: job=%d, spec=%s, err=%v", k, spec, err)
			}
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

// DynamicJobs 必须包级初始化：backup.Init() 在 cronSvr.Start() 之前就会调用 AddJob
// 注册定时备份策略，写一个 nil map 会 panic 直接挂掉服务。
var (
	lock        sync.Mutex
	DynamicJobs = map[string]Job{}
)

func AddJob(id, spec string, fn func() error) {
	log.Logger.Infof("add job: %s, spec: %s", id, spec)
	lock.Lock()
	if old, ok := DynamicJobs[id]; ok {
		if old.status == JOB_SCHEDULED && old.op == JOB_ADD && old.spec == spec {
			lock.Unlock()
			return
		}
		DynamicJobs[id] = Job{
			op:      JOB_UPD,
			spec:    spec,
			fn:      fn,
			status:  JOB_READY,
			entryId: old.entryId,
		}
		lock.Unlock()
		return
	}
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

func addJobFromBackups(self string) {
	backups, err := repository.Ps.GetBackupByShechuleType(model.BACKUP_SCHEDULED)
	if err != nil {
		log.Logger.Errorf("get backup failed: %v", err)
		return
	}
	log.Logger.Infof("found backups: %d", len(backups))
	for _, backup := range backups {
		if self != backup.Instance {
			continue
		}
		log.Logger.Infof("add backup job: %s, spec: %s", backup.BackupId, backup.Crontab)
		AddJob(backup.BackupId, backup.Crontab, func() error {
			return clickhouse.BackupManage(backup.BackupId, self)
		})
	}
}

func (job *CronService) RunDynamicJobs() {
	self := net.JoinHostPort(job.config.Server.Ip, fmt.Sprint(job.config.Server.Port))
	// ckman刚启动时，先把定时任务加上
	addJobFromBackups(self)

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	ticker2 := time.NewTicker(time.Minute * 1)
	defer ticker2.Stop()

	ctx := context.Background()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker2.C:
			// 别的ckman创建的定时任务，需要当前ckman执行的，走这个分支
			addJobFromBackups(self)
		case <-ticker.C:
			// 即时操作的，使用当前ckman创建的定时任务，走这个分支
			lock.Lock()
			for k, v := range DynamicJobs {
				switch v.op {
				case JOB_ADD:
					if v.status == JOB_SCHEDULED {
						continue
					}
					fallthrough
				case JOB_UPD:
					if v.op == JOB_UPD && v.entryId != 0 {
						job.cron.Remove(v.entryId)
						log.Logger.Infof("job removed before update: %s", k)
					}
					entryID, err := job.cron.AddFunc(v.spec, func() {
						_ = v.fn()
					})
					if err != nil {
						log.Logger.Errorf("add dynamic cron job failed: job=%s, spec=%s, err=%v", k, v.spec, err)
						continue
					}
					log.Logger.Infof("job added: %s, spec: %s", k, v.spec)
					v.entryId = entryID
					v.op = JOB_ADD
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
