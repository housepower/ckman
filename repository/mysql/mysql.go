package mysql

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"moul.io/zapgorm2"
)

type MysqlPersistent struct {
	Config   MysqlConfig
	Client   *gorm.DB
	ParentDB *gorm.DB
}

func (mp *MysqlPersistent) Init(config interface{}) error {
	if config == nil {
		config = MysqlConfig{}
	}
	mp.Config = config.(MysqlConfig)
	mp.Config.Normalize()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mp.Config.User,
		mp.Config.Password,
		mp.Config.Host,
		mp.Config.Port,
		mp.Config.DataBase)

	log.Logger.Debugf("mysql dsn:%s", dsn)
	logger := zapgorm2.New(log.ZapLog)
	logger.SetAsDefault()
	db, err := gorm.Open(driver.Open(dsn), &gorm.Config{
		Logger: logger,
	})
	if err != nil {
		return errors.Wrap(err, "")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return errors.Wrap(err, "")
	}

	// set connection pool
	if sqlDB != nil {
		sqlDB.SetMaxIdleConns(mp.Config.MaxIdleConns)
		sqlDB.SetConnMaxIdleTime(time.Second * time.Duration(mp.Config.ConnMaxIdleTime))
		sqlDB.SetMaxOpenConns(mp.Config.MaxOpenConns)
		sqlDB.SetConnMaxLifetime(time.Second * time.Duration(mp.Config.ConnMaxLifetime))
	}
	mp.Client = db
	mp.ParentDB = mp.Client

	//auto create table
	err = mp.Client.Set("gorm:table_options", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4").AutoMigrate(
		&TblCluster{},
		&TblLogic{},
		&TblQueryHistory{},
		&TblTask{},
		&TblBackup{},
		&TblBackupPolicy{}, // 新增
		&TblBackupRun{},    // 新增
	)
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (mp *MysqlPersistent) Begin() error {
	if mp.Client != mp.ParentDB {
		return repository.ErrTransActionBegin
	}
	tx := mp.Client.Begin()
	mp.Client = tx
	return tx.Error
}

func (mp *MysqlPersistent) Rollback() error {
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := mp.Client.Rollback()
	mp.Client = mp.ParentDB
	return tx.Error
}

func (mp *MysqlPersistent) Commit() error {
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := mp.Client.Commit()
	mp.Client = mp.ParentDB
	return tx.Error
}

func (mp *MysqlPersistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
	var config MysqlConfig
	data, err := json.Marshal(configMap)
	if err != nil {
		log.Logger.Errorf("marshal mysql configMap failed:%v", err)
		return nil
	}
	if err = json.Unmarshal(data, &config); err != nil {
		log.Logger.Errorf("unmarshal mysql config failed:%v", err)
		return nil
	}
	return config
}

func (mp *MysqlPersistent) ClusterExists(cluster string) bool {
	_, err := mp.GetClusterbyName(cluster)
	if err != nil {
		return false
	}
	return true
}

func (mp *MysqlPersistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
	var conf model.CKManClickHouseConfig
	var table TblCluster
	tx := mp.Client.Where("cluster_name = ?", cluster).First(&table)
	if tx.Error != nil {
		return model.CKManClickHouseConfig{}, wrapError(tx.Error)
	}
	if err := json.Unmarshal([]byte(table.Config), &conf); err != nil {
		return model.CKManClickHouseConfig{}, errors.Wrap(err, "")
	}
	repository.DecodePasswd(&conf)
	return conf, nil
}

func (mp *MysqlPersistent) GetLogicClusterbyName(logic string) ([]string, error) {
	var table TblLogic
	tx := mp.Client.Where("logic_name = ?", logic).First(&table)
	if tx.Error != nil {
		return []string{}, wrapError(tx.Error)
	}
	physics := strings.Split(table.PhysicClusters, ",")
	return common.ArrayDistinct(physics), nil
}

func (mp *MysqlPersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
	var tables []TblCluster
	clusterMapping := make(map[string]model.CKManClickHouseConfig)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	for _, table := range tables {
		var conf model.CKManClickHouseConfig
		if err := json.Unmarshal([]byte(table.Config), &conf); err != nil {
			return nil, errors.Wrap(err, "")
		}
		repository.DecodePasswd(&conf)
		clusterMapping[table.ClusterName] = conf
	}
	return clusterMapping, nil
}

func (mp *MysqlPersistent) GetAllLogicClusters() (map[string][]string, error) {
	var tables []TblLogic
	logicMapping := make(map[string][]string)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	for _, table := range tables {
		physics := strings.Split(table.PhysicClusters, ",")
		logicMapping[table.LogicCluster] = common.ArrayDistinct(physics)
	}
	return logicMapping, nil
}

func (mp *MysqlPersistent) CreateCluster(conf model.CKManClickHouseConfig) error {
	if _, err := mp.GetClusterbyName(conf.Cluster); err == nil {
		//means already exists
		return repository.ErrRecordExists
	}
	repository.EncodePasswd(&conf)
	config, err := json.Marshal(conf)
	if err != nil {
		return errors.Wrap(err, "")
	}
	table := TblCluster{
		ClusterName: conf.Cluster,
		Config:      string(config),
	}
	tx := mp.Client.Create(&table)
	return tx.Error
}

func (mp *MysqlPersistent) CreateLogicCluster(logic string, physics []string) error {
	if _, err := mp.GetLogicClusterbyName(logic); err == nil {
		//means already exists
		return repository.ErrRecordExists
	}
	table := TblLogic{
		LogicCluster:   logic,
		PhysicClusters: strings.Join(common.ArrayDistinct(physics), ","),
	}
	tx := mp.Client.Create(&table)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) UpdateCluster(conf model.CKManClickHouseConfig) error {
	if _, err := mp.GetClusterbyName(conf.Cluster); err != nil {
		//means not found in database
		return repository.ErrRecordNotFound
	}

	repository.EncodePasswd(&conf)
	config, err := json.Marshal(conf)
	if err != nil {
		return errors.Wrap(err, "")
	}
	table := TblCluster{
		ClusterName: conf.Cluster,
		Config:      string(config),
	}
	tx := mp.Client.Model(TblCluster{}).Where("cluster_name = ?", conf.Cluster).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) UpdateLogicCluster(logic string, physics []string) error {
	if _, err := mp.GetLogicClusterbyName(logic); err != nil {
		//means not found in database
		return repository.ErrRecordNotFound
	}
	table := TblLogic{
		LogicCluster:   logic,
		PhysicClusters: strings.Join(common.ArrayDistinct(physics), ","),
	}
	tx := mp.Client.Model(TblLogic{}).Where("logic_name = ?", logic).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) DeleteCluster(clusterName string) error {
	tx := mp.Client.Where("cluster_name = ?", clusterName).Unscoped().Delete(&TblCluster{})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) DeleteLogicCluster(clusterName string) error {
	tx := mp.Client.Where("logic_name = ?", clusterName).Unscoped().Delete(&TblLogic{})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) GetAllQueryHistory() (map[string]model.QueryHistory, error) {
	var tables []TblQueryHistory
	historys := make(map[string]model.QueryHistory)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	for _, table := range tables {
		history := model.QueryHistory{
			CheckSum:   table.CheckSum,
			Cluster:    table.Cluster,
			QuerySql:   table.QuerySql,
			CreateTime: table.CreateTime,
		}
		historys[table.CheckSum] = history
	}
	return historys, nil
}

func (mp *MysqlPersistent) GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error) {
	var tables []TblQueryHistory
	tx := mp.Client.Where("cluster = ?", cluster).Order("create_time DESC").Limit(100).Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	var historys []model.QueryHistory
	for _, table := range tables {
		history := model.QueryHistory{
			CheckSum:   table.CheckSum,
			Cluster:    table.Cluster,
			QuerySql:   table.QuerySql,
			CreateTime: table.CreateTime,
		}
		historys = append(historys, history)
	}
	return historys, nil
}

func (mp *MysqlPersistent) GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error) {
	var table TblQueryHistory
	tx := mp.Client.Where("checksum = ?", checksum).First(&table)
	if tx.Error != nil {
		return model.QueryHistory{}, wrapError(tx.Error)
	}
	history := model.QueryHistory{
		CheckSum:   table.CheckSum,
		Cluster:    table.Cluster,
		QuerySql:   table.QuerySql,
		CreateTime: table.CreateTime,
	}
	return history, nil
}

func (mp *MysqlPersistent) CreateQueryHistory(qh model.QueryHistory) error {
	table := TblQueryHistory{
		Cluster:    qh.Cluster,
		CheckSum:   qh.CheckSum,
		QuerySql:   qh.QuerySql,
		CreateTime: time.Now(),
	}
	tx := mp.Client.Create(&table)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) UpdateQueryHistory(qh model.QueryHistory) error {
	table := TblQueryHistory{
		Cluster:    qh.Cluster,
		CheckSum:   qh.CheckSum,
		QuerySql:   qh.QuerySql,
		CreateTime: time.Now(),
	}
	tx := mp.Client.Model(TblQueryHistory{}).Where("checksum = ?", qh.CheckSum).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) DeleteQueryHistory(checksum string) error {
	tx := mp.Client.Where("checksum = ?", checksum).Unscoped().Delete(&TblQueryHistory{})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) GetQueryHistoryCount(cluster string) int64 {
	var count int64
	tx := mp.Client.Model(&TblQueryHistory{}).Where("cluster = ?", cluster).Count(&count)
	if tx.Error != nil {
		return 0
	}
	return count
}

func (mp *MysqlPersistent) GetEarliestQuery(cluster string) (model.QueryHistory, error) {
	var table TblQueryHistory
	tx := mp.Client.Order("create_time").Where("cluster = ?", cluster).First(&table)
	if tx.Error != nil {
		return model.QueryHistory{}, wrapError(tx.Error)
	}
	history := model.QueryHistory{
		CheckSum:   table.CheckSum,
		Cluster:    table.Cluster,
		QuerySql:   table.QuerySql,
		CreateTime: table.CreateTime,
	}
	return history, nil
}

func (mp *MysqlPersistent) CreateTask(task model.Task) error {
	config, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "")
	}

	table := TblTask{
		TaskId: task.TaskId,
		Status: task.Status,
		Task:   string(config),
	}
	tx := mp.Client.Create(&table)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	return nil
}

func (mp *MysqlPersistent) UpdateTask(task model.Task) error {
	task.UpdateTime = time.Now()
	config, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "")
	}
	table := TblTask{
		TaskId: task.TaskId,
		Status: task.Status,
		Task:   string(config),
	}
	tx := mp.Client.Model(TblTask{}).Where("task_id = ?", task.TaskId).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) DeleteTask(id string) error {
	tx := mp.Client.Where("task_id = ?", id).Unscoped().Delete(&TblTask{})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) GetAllTasks() ([]model.Task, error) {
	var tables []TblTask
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && !errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return nil, tx.Error
	}
	var tasks []model.Task
	var err error
	for _, table := range tables {
		var task model.Task
		if err = json.Unmarshal([]byte(table.Task), &task); err != nil {
			return []model.Task{}, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (mp *MysqlPersistent) GetEffectiveTaskCount() int64 {
	var count int64
	tx := mp.Client.Model(&TblTask{}).Where("status in ?", []int{model.TaskStatusRunning, model.TaskStatusWaiting}).Count(&count)
	if tx.Error != nil {
		count = 0
	}
	return count
}

func (mp *MysqlPersistent) GetPengdingTasks(serverIp string) ([]model.Task, error) {
	var tables []TblTask
	tx := mp.Client.Where("status = ?", model.TaskStatusWaiting).Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	var tasks []model.Task
	var err error
	for _, table := range tables {
		var task model.Task
		if err = json.Unmarshal([]byte(table.Task), &task); err != nil {
			return []model.Task{}, errors.Wrap(err, "")
		}
		if task.ServerIp == serverIp {
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func (mp *MysqlPersistent) GetTaskbyTaskId(id string) (model.Task, error) {
	var table TblTask
	tx := mp.Client.Where("task_id = ?", id).First(&table)
	if tx.Error != nil {
		return model.Task{}, wrapError(tx.Error)
	}
	var task model.Task
	if err := json.Unmarshal([]byte(table.Task), &task); err != nil {
		return model.Task{}, errors.Wrap(err, "")
	}

	return task, nil
}

func (mp *MysqlPersistent) CreateBackup(backup model.Backup) error {
	backup.CreateTime = time.Now()
	backup.UpdateTime = backup.CreateTime

	var tbackup TblBackup
	tbackup.BackupId = backup.BackupId
	tbackup.ClusterName = backup.ClusterName
	tbackup.UpdateTime = backup.UpdateTime.Format("2006-01-02 15:04:05.999")
	raw, err := json.Marshal(backup)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbackup.Backup = string(raw)
	tx := mp.Client.Create(&tbackup)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	return nil
}
func (mp *MysqlPersistent) UpdateBackup(backup model.Backup) error {
	backup.UpdateTime = time.Now()

	var tbackup TblBackup
	tbackup.BackupId = backup.BackupId
	tbackup.ClusterName = backup.ClusterName
	tbackup.UpdateTime = backup.UpdateTime.Format("2006-01-02 15:04:05.999")
	raw, err := json.Marshal(backup)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbackup.Backup = string(raw)
	tx := mp.Client.Model(TblBackup{}).Where("backup_id =?", backup.BackupId).Updates(&tbackup)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	return nil
}
func (mp *MysqlPersistent) DeleteBackup(id string) error {
	var tbackup TblBackup
	tx := mp.Client.Where("backup_id =?", id).Delete(&tbackup)
	return wrapError(tx.Error)
}
func (mp *MysqlPersistent) GetAllBackups(cluster string) ([]model.Backup, error) {
	var tbackups []TblBackup
	tx := mp.Client.Where("cluster_name =?", cluster).Order("update_time DESC").Find(&tbackups)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	var backups []model.Backup
	for _, tbackup := range tbackups {
		var backup model.Backup
		err := json.Unmarshal([]byte(tbackup.Backup), &backup)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}
		backups = append(backups, backup)
	}
	return backups, nil
}
func (mp *MysqlPersistent) GetBackupById(id string) (model.Backup, error) {
	var tbackup TblBackup
	tx := mp.Client.Where("backup_id =?", id).First(&tbackup)
	if tx.Error != nil {
		return model.Backup{}, wrapError(tx.Error)
	}
	var backup model.Backup
	if err := json.Unmarshal([]byte(tbackup.Backup), &backup); err != nil {
		return model.Backup{}, errors.Wrap(err, "")
	}
	return backup, nil
}
func (mp *MysqlPersistent) GetBackupByTable(cluster, database, table string) (model.Backup, error) {
	var tbackups []TblBackup
	tx := mp.Client.Where("cluster_name =?", cluster).Order("update_time").Find(&tbackups)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return model.Backup{}, errors.Wrap(tx.Error, "")
	}
	for _, tbackup := range tbackups {
		var backup model.Backup
		err := json.Unmarshal([]byte(tbackup.Backup), &backup)
		if err != nil {
			return model.Backup{}, errors.Wrap(err, "")
		}
		if backup.Database == database && backup.Table == table {
			return backup, nil
		}
	}
	return model.Backup{}, repository.ErrRecordNotFound
}
func (mp *MysqlPersistent) GetbackupByOperation(operation string) ([]model.Backup, error) {
	var tbackups []TblBackup
	tx := mp.Client.Order("update_time").Find(&tbackups)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	var backups []model.Backup
	for _, tbackup := range tbackups {
		var backup model.Backup
		err := json.Unmarshal([]byte(tbackup.Backup), &backup)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}
		if backup.Operation == operation {
			backups = append(backups, backup)
		}
	}
	return backups, nil
}

func (mp *MysqlPersistent) GetBackupByShechuleType(scheduleType string) ([]model.Backup, error) {
	var tbackups []TblBackup
	tx := mp.Client.Order("update_time").Find(&tbackups)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	var backups []model.Backup
	for _, tbackup := range tbackups {
		var backup model.Backup
		err := json.Unmarshal([]byte(tbackup.Backup), &backup)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}
		if backup.ScheduleType == scheduleType {
			backups = append(backups, backup)
		}
	}
	return backups, nil
}

// ─── PersistentBackupPolicyService ───────────────────────────────────────────

func (mp *MysqlPersistent) CreateBackupPolicy(p model.BackupPolicy) error {
	p.CreateTime = time.Now()
	p.UpdateTime = p.CreateTime
	raw, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblBackupPolicy{
		PolicyID:     p.PolicyID,
		ClusterName:  p.ClusterName,
		Database:     p.Database,
		Table:        p.Table,
		Instance:     p.Instance,
		ScheduleType: p.ScheduleType,
		Enabled:      p.Enabled,
		Deleted:      false,
		Policy:       string(raw),
		UpdateTime:   p.UpdateTime.Format("2006-01-02 15:04:05.999"),
	}
	tx := mp.Client.Create(&tbl)
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) UpdateBackupPolicy(p model.BackupPolicy) error {
	p.UpdateTime = time.Now()
	raw, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := mp.Client.Model(&TblBackupPolicy{}).Where("policy_id = ?", p.PolicyID).Updates(map[string]interface{}{
		"cluster_name":  p.ClusterName,
		"database_name": p.Database,
		"table_name":    p.Table,
		"instance":      p.Instance,
		"schedule_type": p.ScheduleType,
		"enabled":       p.Enabled,
		"deleted":       p.Deleted,
		"policy":        string(raw),
		"update_time":   p.UpdateTime.Format("2006-01-02 15:04:05.999"),
	})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) DeleteBackupPolicy(policyID string) error {
	tx := mp.Client.Model(&TblBackupPolicy{}).Where("policy_id = ?", policyID).Updates(map[string]interface{}{
		"deleted":     true,
		"enabled":     false,
		"update_time": time.Now().Format("2006-01-02 15:04:05.999"),
	})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) GetBackupPolicy(policyID string) (model.BackupPolicy, error) {
	var tbl TblBackupPolicy
	if err := mp.Client.Where("policy_id = ?", policyID).First(&tbl).Error; err != nil {
		return model.BackupPolicy{}, wrapError(err)
	}
	var p model.BackupPolicy
	if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
		return model.BackupPolicy{}, errors.Wrap(err, "")
	}
	return p, nil
}

func (mp *MysqlPersistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	var tbls []TblBackupPolicy
	tx := mp.Client.Where("cluster_name = ? AND deleted = ?", cluster, false).Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	policies := make([]model.BackupPolicy, 0, len(tbls))
	for _, tbl := range tbls {
		var p model.BackupPolicy
		if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
			return nil, errors.Wrap(err, "")
		}
		policies = append(policies, p)
	}
	return policies, nil
}

func (mp *MysqlPersistent) GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) {
	var tbls []TblBackupPolicy
	tx := mp.Client.Where("instance = ? AND enabled = ? AND schedule_type = ? AND deleted = ?",
		instance, true, "scheduled", false).Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	policies := make([]model.BackupPolicy, 0, len(tbls))
	for _, tbl := range tbls {
		var p model.BackupPolicy
		if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
			return nil, errors.Wrap(err, "")
		}
		policies = append(policies, p)
	}
	return policies, nil
}

// ─── PersistentBackupRunService ──────────────────────────────────────────────

func (mp *MysqlPersistent) CreateBackupRun(r model.BackupRun) error {
	r.CreateTime = time.Now()
	raw, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblBackupRun{
		RunID:       r.RunID,
		PolicyID:    r.PolicyID,
		ClusterName: r.ClusterName,
		Database:    r.Database,
		Table:       r.Table,
		Status:      r.Status,
		Instance:    r.Instance,
		StartedAt:   r.StartedAt,
		Run:         string(raw),
		CreateTime:  r.CreateTime,
	}
	return wrapError(mp.Client.Create(&tbl).Error)
}

func (mp *MysqlPersistent) UpdateBackupRun(r model.BackupRun) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := mp.Client.Model(&TblBackupRun{}).Where("run_id = ?", r.RunID).Updates(map[string]interface{}{
		"status":     r.Status,
		"instance":   r.Instance,
		"started_at": r.StartedAt,
		"run":        string(raw),
	})
	return wrapError(tx.Error)
}

func (mp *MysqlPersistent) GetBackupRun(runID string) (model.BackupRun, error) {
	var tbl TblBackupRun
	if err := mp.Client.Where("run_id = ?", runID).First(&tbl).Error; err != nil {
		return model.BackupRun{}, wrapError(err)
	}
	var r model.BackupRun
	if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
		return model.BackupRun{}, errors.Wrap(err, "")
	}
	return r, nil
}

func (mp *MysqlPersistent) GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
	db := mp.Client.Where("policy_id = ?", policyID).Order("started_at DESC")
	if !before.IsZero() {
		db = db.Where("started_at < ?", before)
	}
	if limit > 0 {
		db = db.Limit(limit)
	}
	var tbls []TblBackupRun
	if err := db.Find(&tbls).Error; err != nil && err != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(err, "")
	}
	runs := make([]model.BackupRun, 0, len(tbls))
	for _, tbl := range tbls {
		var r model.BackupRun
		if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
			return nil, errors.Wrap(err, "")
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (mp *MysqlPersistent) GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error) {
	cutoff := time.Now().AddDate(0, 0, -sinceDays)
	var tbls []TblBackupRun
	tx := mp.Client.Where(
		"cluster_name = ? AND database_name = ? AND table_name = ? AND started_at > ?",
		cluster, database, table, cutoff,
	).Order("started_at DESC").Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	runs := make([]model.BackupRun, 0, len(tbls))
	for _, tbl := range tbls {
		var r model.BackupRun
		if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
			return nil, errors.Wrap(err, "")
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (mp *MysqlPersistent) GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error) {
	var tbls []TblBackupRun
	tx := mp.Client.Where("policy_id = ? AND status IN ?", policyID,
		[]string{model.BACKUP_STATUS_QUEUED, model.BACKUP_STATUS_RUNNING}).Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	runs := make([]model.BackupRun, 0, len(tbls))
	for _, tbl := range tbls {
		var r model.BackupRun
		if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
			return nil, errors.Wrap(err, "")
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (mp *MysqlPersistent) GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error) {
	var tbls []TblBackupRun
	tx := mp.Client.Where("instance = ? AND status IN ?", instance,
		[]string{model.BACKUP_STATUS_QUEUED, model.BACKUP_STATUS_RUNNING}).Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	runs := make([]model.BackupRun, 0, len(tbls))
	for _, tbl := range tbls {
		var r model.BackupRun
		if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
			return nil, errors.Wrap(err, "")
		}
		runs = append(runs, r)
	}
	return runs, nil
}

func (mp *MysqlPersistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
	// 先取出当前记录，做早返回优化（常见路径：无人竞争时避免一次 UPDATE）
	var tr TblBackupRun
	if err := mp.Client.Where("run_id = ?", runID).First(&tr).Error; err != nil {
		return false, wrapError(err)
	}
	var run model.BackupRun
	if err := json.Unmarshal([]byte(tr.Run), &run); err != nil {
		return false, errors.Wrap(err, "")
	}
	if run.Status != model.BACKUP_STATUS_QUEUED {
		return false, nil
	}
	// 更新 model 字段后重新 marshal 到 blob
	run.Status = model.BACKUP_STATUS_RUNNING
	run.Instance = instance
	run.StartedAt = startedAt
	raw, err := json.Marshal(run)
	if err != nil {
		return false, errors.Wrap(err, "")
	}
	// 原子条件 UPDATE：并发抢占时 WHERE status='queued' 保证只有一个 worker 成功
	tx := mp.Client.Model(&TblBackupRun{}).
		Where("run_id = ? AND status = ?", runID, model.BACKUP_STATUS_QUEUED).
		Updates(map[string]interface{}{
			"status":     model.BACKUP_STATUS_RUNNING,
			"instance":   instance,
			"started_at": startedAt,
			"run":        string(raw),
		})
	if tx.Error != nil {
		return false, wrapError(tx.Error)
	}
	return tx.RowsAffected == 1, nil
}

func wrapError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = repository.ErrRecordNotFound
	}
	return err
}

func NewMysqlPersistent() *MysqlPersistent {
	return &MysqlPersistent{}
}
