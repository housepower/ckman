package dm8

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
	dmSchema "github.com/wanlay/gorm-dm8/schema"
	"gorm.io/gorm"
	"moul.io/zapgorm2"
)

type DM8Persistent struct {
	Config   DM8Config
	Client   *gorm.DB
	ParentDB *gorm.DB
}

func (mp *DM8Persistent) Init(config interface{}) error {
	if config == nil {
		config = DM8Config{}
	}
	mp.Config = config.(DM8Config)
	mp.Config.Normalize()
	cfgMap := map[string]interface{}{
		"host":     mp.Config.Host,
		"port":     mp.Config.Port,
		"user":     mp.Config.User,
		"password": mp.Config.Password,
	}
	dialector, err := BuildDialector(cfgMap)
	if err != nil {
		return errors.Wrap(err, "")
	}
	log.Logger.Debugf("DM8 dsn: using BuildDialector")
	logger := zapgorm2.New(log.ZapLog)
	logger.SetAsDefault()
	db, err := gorm.Open(dialector, &gorm.Config{
		Logger:                                   logger,
		DisableForeignKeyConstraintWhenMigrating: true,
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
	err = mp.Client.AutoMigrate(
		&TblCluster{},
		&TblLogic{},
		&TblQueryHistory{},
		&TblTask{},
		&TblBackup{},
		&TblBackupPolicy{},
		&TblBackupRun{},
		&TblUser{}, // Phase 1 用户管理
	)
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (mp *DM8Persistent) Begin() error {
	if mp.Client != mp.ParentDB {
		return repository.ErrTransActionBegin
	}
	tx := mp.Client.Begin()
	mp.Client = tx
	return tx.Error
}

func (mp *DM8Persistent) Rollback() error {
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := mp.Client.Rollback()
	mp.Client = mp.ParentDB
	return tx.Error
}

func (mp *DM8Persistent) Commit() error {
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := mp.Client.Commit()
	mp.Client = mp.ParentDB
	return tx.Error
}

func (mp *DM8Persistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
	var config DM8Config
	data, err := json.Marshal(configMap)
	if err != nil {
		log.Logger.Errorf("marshal DM8 configMap failed:%v", err)
		return nil
	}
	if err = json.Unmarshal(data, &config); err != nil {
		log.Logger.Errorf("unmarshal DM8 config failed:%v", err)
		return nil
	}
	return config
}

func (mp *DM8Persistent) ClusterExists(cluster string) bool {
	_, err := mp.GetClusterbyName(cluster)
	if err != nil {
		return false
	}
	return true
}

func (mp *DM8Persistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
	var conf model.CKManClickHouseConfig
	var table TblCluster
	tx := mp.Client.Where("cluster_name = ?", cluster).First(&table)
	if tx.Error != nil {
		return model.CKManClickHouseConfig{}, wrapError(tx.Error)
	}
	if err := json.Unmarshal([]byte(table.Configuration), &conf); err != nil {
		return model.CKManClickHouseConfig{}, errors.Wrap(err, "")
	}
	repository.DecodePasswd(&conf)
	return conf, nil
}

func (mp *DM8Persistent) GetLogicClusterbyName(logic string) ([]string, error) {
	var table TblLogic
	tx := mp.Client.Where("logic_name = ?", logic).First(&table)
	if tx.Error != nil {
		return []string{}, wrapError(tx.Error)
	}
	physics := strings.Split(table.PhysicClusters, ",")
	return common.ArrayDistinct(physics), nil
}

func (mp *DM8Persistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
	var tables []TblCluster
	clusterMapping := make(map[string]model.CKManClickHouseConfig)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	for _, table := range tables {
		var conf model.CKManClickHouseConfig
		if err := json.Unmarshal([]byte(table.Configuration), &conf); err != nil {
			return nil, errors.Wrap(err, "")
		}
		repository.DecodePasswd(&conf)
		clusterMapping[table.ClusterName] = conf
	}
	return clusterMapping, nil
}

func (mp *DM8Persistent) GetAllLogicClusters() (map[string][]string, error) {
	var tables []TblLogic
	logicMapping := make(map[string][]string)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	for _, table := range tables {
		physics := strings.Split(table.PhysicClusters, ",")
		logicMapping[table.LogicName] = common.ArrayDistinct(physics)
	}
	return logicMapping, nil
}

func (mp *DM8Persistent) CreateCluster(conf model.CKManClickHouseConfig) error {
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
		ClusterName:   conf.Cluster,
		Configuration: dmSchema.Clob(config),
	}
	tx := mp.Client.Create(&table)
	return tx.Error
}

func (mp *DM8Persistent) CreateLogicCluster(logic string, physics []string) error {
	if _, err := mp.GetLogicClusterbyName(logic); err == nil {
		//means already exists
		return repository.ErrRecordExists
	}
	table := TblLogic{
		LogicName:      logic,
		PhysicClusters: strings.Join(common.ArrayDistinct(physics), ","),
	}
	tx := mp.Client.Create(&table)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) UpdateCluster(conf model.CKManClickHouseConfig) error {
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
		ClusterName:   conf.Cluster,
		Configuration: dmSchema.Clob(config),
	}
	tx := mp.Client.Model(TblCluster{}).Where("cluster_name = ?", conf.Cluster).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) UpdateLogicCluster(logic string, physics []string) error {
	if _, err := mp.GetLogicClusterbyName(logic); err != nil {
		//means not found in database
		return repository.ErrRecordNotFound
	}
	table := TblLogic{
		LogicName:      logic,
		PhysicClusters: strings.Join(common.ArrayDistinct(physics), ","),
	}
	tx := mp.Client.Model(TblLogic{}).Where("logic_name = ?", logic).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) DeleteCluster(clusterName string) error {
	tx := mp.Client.Where("cluster_name = ?", clusterName).Unscoped().Delete(&TblCluster{})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) DeleteLogicCluster(clusterName string) error {
	tx := mp.Client.Where("logic_name = ?", clusterName).Unscoped().Delete(&TblLogic{})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) GetAllQueryHistory() (map[string]model.QueryHistory, error) {
	var tables []TblQueryHistory
	historys := make(map[string]model.QueryHistory)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	for _, table := range tables {
		history := model.QueryHistory{
			CheckSum:   table.Checksum,
			Cluster:    table.ClusterName,
			QuerySql:   string(table.Query),
			CreateTime: table.CreateTime,
		}
		historys[table.Checksum] = history
	}
	return historys, nil
}

func (mp *DM8Persistent) GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error) {
	var tables []TblQueryHistory
	tx := mp.Client.Where("cluster_name = ?", cluster).Order("create_time DESC").Limit(100).Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	var historys []model.QueryHistory
	for _, table := range tables {
		history := model.QueryHistory{
			CheckSum:   table.Checksum,
			Cluster:    table.ClusterName,
			QuerySql:   string(table.Query),
			CreateTime: table.CreateTime,
		}
		historys = append(historys, history)
	}
	return historys, nil
}

func (mp *DM8Persistent) GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error) {
	var table TblQueryHistory
	tx := mp.Client.Where("checksum = ?", checksum).First(&table)
	if tx.Error != nil {
		return model.QueryHistory{}, wrapError(tx.Error)
	}
	history := model.QueryHistory{
		CheckSum:   table.Checksum,
		Cluster:    table.ClusterName,
		QuerySql:   string(table.Query),
		CreateTime: table.CreateTime,
	}
	return history, nil
}

func (mp *DM8Persistent) CreateQueryHistory(qh model.QueryHistory) error {
	table := TblQueryHistory{
		ClusterName: qh.Cluster,
		Checksum:    qh.CheckSum,
		Query:       dmSchema.Clob(qh.QuerySql),
		CreateTime:  time.Now(),
	}
	tx := mp.Client.Create(&table)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) UpdateQueryHistory(qh model.QueryHistory) error {
	table := TblQueryHistory{
		ClusterName: qh.Cluster,
		Checksum:    qh.CheckSum,
		Query:       dmSchema.Clob(qh.QuerySql),
		CreateTime:  time.Now(),
	}
	tx := mp.Client.Model(TblQueryHistory{}).Where("checksum = ?", qh.CheckSum).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) DeleteQueryHistory(checksum string) error {
	tx := mp.Client.Where("checksum = ?", checksum).Unscoped().Delete(&TblQueryHistory{})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) GetQueryHistoryCount(cluster string) int64 {
	var count int64
	tx := mp.Client.Model(&TblQueryHistory{}).Where("cluster_name = ?", cluster).Count(&count)
	if tx.Error != nil {
		return 0
	}
	return count
}

func (mp *DM8Persistent) GetEarliestQuery(cluster string) (model.QueryHistory, error) {
	var table TblQueryHistory
	tx := mp.Client.Where("cluster_name = ?", cluster).Order("create_time").First(&table)
	if tx.Error != nil {
		return model.QueryHistory{}, wrapError(tx.Error)
	}
	history := model.QueryHistory{
		CheckSum:   table.Checksum,
		Cluster:    table.ClusterName,
		QuerySql:   string(table.Query),
		CreateTime: table.CreateTime,
	}
	return history, nil
}

func (mp *DM8Persistent) CreateTask(task model.Task) error {
	config, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "")
	}

	table := TblTask{
		TaskId: task.TaskId,
		Status: task.Status,
		Task:   dmSchema.Clob(config),
	}
	tx := mp.Client.Create(&table)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	return nil
}

func (mp *DM8Persistent) UpdateTask(task model.Task) error {
	task.UpdateTime = time.Now()
	config, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "")
	}
	table := TblTask{
		TaskId: task.TaskId,
		Status: task.Status,
		Task:   dmSchema.Clob(config),
	}
	tx := mp.Client.Model(TblTask{}).Where("task_id = ?", task.TaskId).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) DeleteTask(id string) error {
	tx := mp.Client.Where("task_id = ?", id).Unscoped().Delete(&TblTask{})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) GetAllTasks() ([]model.Task, error) {
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

func (mp *DM8Persistent) GetEffectiveTaskCount() int64 {
	var count int64
	tx := mp.Client.Model(&TblTask{}).Where("status in ?", []int{model.TaskStatusRunning, model.TaskStatusWaiting}).Count(&count)
	if tx.Error != nil {
		count = 0
	}
	return count
}

func (mp *DM8Persistent) GetPengdingTasks(serverIp string) ([]model.Task, error) {
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

func (mp *DM8Persistent) GetTaskbyTaskId(id string) (model.Task, error) {
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

func (mp *DM8Persistent) CreateBackup(backup model.Backup) error {
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
	tbackup.Backup = dmSchema.Clob(raw)
	tx := mp.Client.Create(&tbackup)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	return nil
}
func (mp *DM8Persistent) UpdateBackup(backup model.Backup) error {
	backup.UpdateTime = time.Now()

	var tbackup TblBackup
	tbackup.BackupId = backup.BackupId
	tbackup.ClusterName = backup.ClusterName
	tbackup.UpdateTime = backup.UpdateTime.Format("2006-01-02 15:04:05.999")
	raw, err := json.Marshal(backup)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbackup.Backup = dmSchema.Clob(raw)
	tx := mp.Client.Model(TblBackup{}).Where("backup_id =?", backup.BackupId).Updates(&tbackup)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	return nil
}
func (mp *DM8Persistent) DeleteBackup(id string) error {
	var tbackup TblBackup
	tx := mp.Client.Where("backup_id =?", id).Delete(&tbackup)
	return wrapError(tx.Error)
}
func (mp *DM8Persistent) GetAllBackups(cluster string) ([]model.Backup, error) {
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
func (mp *DM8Persistent) GetBackupById(id string) (model.Backup, error) {
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
func (mp *DM8Persistent) GetBackupByTable(cluster, database, table string) (model.Backup, error) {
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
func (mp *DM8Persistent) GetbackupByOperation(operation string) ([]model.Backup, error) {
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

func (mp *DM8Persistent) GetBackupByShechuleType(scheduleType string) ([]model.Backup, error) {
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

func (mp *DM8Persistent) CreateBackupPolicy(p model.BackupPolicy) error {
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
		Policy:       dmSchema.Clob(string(raw)),
		UpdateTime:   p.UpdateTime.Format("2006-01-02 15:04:05.999"),
	}
	tx := mp.Client.Create(&tbl)
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) UpdateBackupPolicy(p model.BackupPolicy) error {
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
		"policy":        dmSchema.Clob(string(raw)),
		"update_time":   p.UpdateTime.Format("2006-01-02 15:04:05.999"),
	})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) DeleteBackupPolicy(policyID string) error {
	tx := mp.Client.Model(&TblBackupPolicy{}).
		Where("policy_id = ?", policyID).
		Updates(map[string]interface{}{
			"deleted":     true,
			"enabled":     false,
			"update_time": time.Now().Format("2006-01-02 15:04:05.999"),
		})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (mp *DM8Persistent) GetBackupPolicy(policyID string) (model.BackupPolicy, error) {
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

func (mp *DM8Persistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
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

func (mp *DM8Persistent) GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) {
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

func (mp *DM8Persistent) CreateBackupRun(r model.BackupRun) error {
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
		Run:         dmSchema.Clob(string(raw)),
		CreateTime:  r.CreateTime,
	}
	return wrapError(mp.Client.Create(&tbl).Error)
}

func (mp *DM8Persistent) UpdateBackupRun(r model.BackupRun) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := mp.Client.Model(&TblBackupRun{}).Where("run_id = ?", r.RunID).Updates(map[string]interface{}{
		"status":     r.Status,
		"instance":   r.Instance,
		"started_at": r.StartedAt,
		"run":        dmSchema.Clob(string(raw)),
	})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) DeleteBackupRun(runID string) error {
	tx := mp.Client.Where("run_id = ?", runID).Delete(&TblBackupRun{})
	return wrapError(tx.Error)
}

func (mp *DM8Persistent) GetBackupRun(runID string) (model.BackupRun, error) {
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

func (mp *DM8Persistent) GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
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

func (mp *DM8Persistent) GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error) {
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

func (mp *DM8Persistent) GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error) {
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

func (mp *DM8Persistent) GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error) {
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

func (mp *DM8Persistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
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
	run.Status = model.BACKUP_STATUS_RUNNING
	run.Instance = instance
	run.StartedAt = startedAt
	raw, err := json.Marshal(run)
	if err != nil {
		return false, errors.Wrap(err, "")
	}
	tx := mp.Client.Model(&TblBackupRun{}).
		Where("run_id = ? AND status = ?", runID, model.BACKUP_STATUS_QUEUED).
		Updates(map[string]interface{}{
			"status":     model.BACKUP_STATUS_RUNNING,
			"instance":   instance,
			"started_at": startedAt,
			"run":        dmSchema.Clob(string(raw)),
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

func (mp *DM8Persistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
	var tbls []TblBackupPolicy
	tx := mp.Client.Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	out := make([]model.BackupPolicy, 0, len(tbls))
	for _, tbl := range tbls {
		var p model.BackupPolicy
		if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
			return nil, errors.Wrap(err, "")
		}
		out = append(out, p)
	}
	return out, nil
}

func (mp *DM8Persistent) GetAllBackupRuns() ([]model.BackupRun, error) {
	var tbls []TblBackupRun
	tx := mp.Client.Find(&tbls)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, errors.Wrap(tx.Error, "")
	}
	out := make([]model.BackupRun, 0, len(tbls))
	for _, tbl := range tbls {
		var r model.BackupRun
		if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
			return nil, errors.Wrap(err, "")
		}
		out = append(out, r)
	}
	return out, nil
}

// ─── User ─────────────────────────────────────────────────────────────────────

func (mp *DM8Persistent) UserExists(username string) bool {
	_, err := mp.GetUserByName(username)
	return err == nil
}

func (mp *DM8Persistent) GetUserByName(username string) (model.CkmanUser, error) {
	var tbl TblUser
	if err := mp.Client.Where("username = ?", username).First(&tbl).Error; err != nil {
		return model.CkmanUser{}, wrapError(err)
	}
	return tblUserToModel(tbl), nil
}

func (mp *DM8Persistent) GetAllUsers() ([]model.CkmanUser, error) {
	var tbls []TblUser
	if err := mp.Client.Order("username ASC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.CkmanUser, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, tblUserToModel(t))
	}
	return out, nil
}

func (mp *DM8Persistent) CreateUser(u model.CkmanUser) error {
	tbl := TblUser{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Policy:       u.Policy,
		Enabled:      u.Enabled,
	}
	if err := mp.Client.Create(&tbl).Error; err != nil {
		if isUniqueViolationDM8(err) {
			return repository.ErrRecordExists
		}
		return wrapError(err)
	}
	return nil
}

func (mp *DM8Persistent) UpdateUser(u model.CkmanUser) error {
	updates := map[string]interface{}{
		"password_hash": u.PasswordHash,
		"policy":        u.Policy,
		"enabled":       u.Enabled,
	}
	tx := mp.Client.Model(&TblUser{}).Where("username = ?", u.Username).Updates(updates)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (mp *DM8Persistent) DeleteUser(username string) error {
	tx := mp.Client.Unscoped().Where("username = ?", username).Delete(&TblUser{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func isUniqueViolationDM8(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ORA-00001") ||
		strings.Contains(msg, "唯一性约束") ||
		strings.Contains(msg, "unique constraint")
}

func NewDM8Persistent() *DM8Persistent {
	return &DM8Persistent{}
}
