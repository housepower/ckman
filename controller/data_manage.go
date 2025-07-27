package controller

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/cron"
)

type DataManageController struct {
	Controller
}

func NewDataManageController(wrapfunc Wrapfunc) *DataManageController {
	return &DataManageController{
		Controller: Controller{
			wrapfunc: wrapfunc,
		},
	}
}

func (controller *DataManageController) BackupData(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	var req model.BackupRequest
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	if req.ScheduleType == model.BACKUP_SCHEDULED {
		if req.Crontab == "" {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Errorf("crontab is empty"))
			return
		}
		if req.DaysBefore <= 0 {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Errorf("days before must be greater than 0"))
			return
		}
		if req.BackupStyle == model.BACKUP_BY_PARTITON {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Errorf("partition backup can not be scheduled"))
			return
		}

		if req.BackupType == model.BACKUP_TYPE_FULL {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Errorf("full backup can not be scheduled"))
			return
		}
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	svc := clickhouse.NewCkService(&conf)
	if err = svc.InitCkService(); err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	for _, table := range req.Tables {
		var cnt uint64
		err = svc.Conn.QueryRow(fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'", req.Database, table)).Scan(&cnt)
		if err != nil {
			controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
			return
		}
		if cnt == 0 {
			controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, fmt.Errorf("table %s.%s not exist", req.Database, table))
			return
		}
		if req.BackupStyle == model.BACKUP_BY_PARTITON {
			for _, partition := range req.Partitions {
				var cnt uint64
				err = svc.Conn.QueryRow(fmt.Sprintf("SELECT count() FROM system.parts WHERE database = '%s' AND table = '%s' AND partition = '%s'", req.Database, table, partition)).Scan(&cnt)
				if err != nil {
					controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
					return
				}
				if cnt == 0 {
					controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, fmt.Errorf("partition %s in table %s.%s not exist", partition, req.Database, table))
					return
				}
			}
		}
	}
	backups := NewBackup(clusterName, req)
	for _, backup := range backups {
		if err := repository.Ps.CreateBackup(backup); err != nil {
			controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
			return
		}
		if backup.ScheduleType == model.BACKUP_IMMEDIATE {
			if err := clickhouse.BackupManage(backup.BackupId); err != nil {
				controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
				return
			}
		} else if backup.ScheduleType == model.BACKUP_SCHEDULED {
			cron.AddJob(backup.BackupId, backup.Crontab, func() error {
				return clickhouse.BackupManage(backup.BackupId)
			})
		}
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

func (controller *DataManageController) RestoreData(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RestoreRequest
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	backup, err := repository.Ps.GetBackupById(req.BackupId)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	var newBack model.Backup
	newBack.BackupId = uuid.New()
	newBack.Database = backup.Database
	newBack.Table = backup.Table
	newBack.ScheduleType = model.BACKUP_IMMEDIATE
	newBack.Operation = model.OP_RESTORE
	newBack.Clean = false
	newBack.ClusterName = clusterName
	newBack.TargetType = backup.TargetType
	newBack.DaysBefore = 0
	newBack.S3 = backup.S3
	newBack.Local = backup.Local
	newBack.Status = model.BACKUP_STATUS_WAITING
	var partitions []model.BackupLists
	for _, partition := range req.Partitions {
		found := false
		var pp model.BackupLists
		for _, p := range backup.Partitions {
			if p.Partition == partition && p.Status == model.BACKUP_PARTITION_STATUS_SUCCESS {
				found = true
				pp = p
				break
			}
		}
		if !found {
			controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, fmt.Errorf("partition %s not exist", partition))
			continue
		}
		partitions = append(partitions, model.BackupLists{
			//QueryId:   uuid.New(),
			Partition: partition,
			Status:    model.BACKUP_PARTITION_STATUS_WAITING,
			Rows:      pp.Rows,
			Size:      pp.Size,
			FileNum:   pp.FileNum,
		})
	}
	newBack.Partitions = partitions
	if err := repository.Ps.CreateBackup(newBack); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	if err := clickhouse.BackupManage(newBack.BackupId); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

func (controller *DataManageController) GetBackupHistory(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	backups, err := repository.Ps.GetAllBackups(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, backups)
}

func (controller *DataManageController) DeleteBackup(c *gin.Context) {
	backupId := c.Param("backupId")
	backup, err := repository.Ps.GetBackupById(backupId)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	if backup.ScheduleType == model.BACKUP_SCHEDULED {
		cron.RemoveJob(backup.BackupId)
	}
	if err := repository.Ps.DeleteBackup(backupId); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

func (controller *DataManageController) GetBackupById(c *gin.Context) {
	backupId := c.Param("backupId")
	backup, err := repository.Ps.GetBackupById(backupId)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, backup)
}

func NewBackup(clusterName string, req model.BackupRequest) []model.Backup {
	var backups []model.Backup
	for _, table := range req.Tables {
		var back model.Backup
		back.BackupId = uuid.New()
		back.ScheduleType = req.ScheduleType
		back.ClusterName = clusterName
		back.Operation = model.OP_BACKUP
		back.Database = req.Database
		back.Table = table
		back.DaysBefore = req.DaysBefore
		back.Clean = req.Clean
		back.Compression = req.Compression
		back.Status = model.BACKUP_STATUS_WAITING
		if req.BackupStyle == model.BACKUP_BY_PARTITON {
			var partitions []model.BackupLists
			for _, p := range req.Partitions {
				if p == "tuple()" {
					p = "all"
				}
				partitions = append(partitions, model.BackupLists{
					//QueryId:   uuid.New(),
					Partition: p,
					Status:    model.BACKUP_PARTITION_STATUS_WAITING,
				})
			}
			back.Partitions = partitions
		}
		if req.BackupStyle == model.BACKUP_BY_PARTITON {
			back.DaysBefore = 0
		} else {
			back.DaysBefore = req.DaysBefore
			back.Crontab = req.Crontab
		}
		if req.BackupType == model.BACKUP_TYPE_FULL {
			back.Partitions = []model.BackupLists{
				model.BackupLists{
					//QueryId:   uuid.New(),
					Partition: "all",
					Status:    model.BACKUP_PARTITION_STATUS_WAITING,
				},
			}
		}
		back.TargetType = req.Target
		if req.Target == model.BACKUP_LOCAL {
			back.Local = req.Local
		} else if req.Target == model.BACKUP_S3 {
			back.S3 = req.S3
		}
		backups = append(backups, back)
	}
	return backups
}
