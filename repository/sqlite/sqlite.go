package sqlite

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"moul.io/zapgorm2"
)

type SQLitePersistent struct {
	Config   LocalConfig
	Client   *gorm.DB
	ParentDB *gorm.DB
}

func NewSQLitePersistent() *SQLitePersistent {
	return &SQLitePersistent{}
}

// AttachReadOnly binds an externally-opened (typically read-only) gorm.DB
// to this SQLitePersistent instance. Used exclusively by the dump-to-json
// tool to read SQLite without triggering migration or write paths.
// Do NOT use from business code.
func (sp *SQLitePersistent) AttachReadOnly(db *gorm.DB) {
	sp.Client = db
	sp.ParentDB = db
}

func (sp *SQLitePersistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
	var cfg LocalConfig
	data, err := json.Marshal(configMap)
	if err != nil {
		log.Logger.Errorf("marshal sqlite configMap failed: %v", err)
		return nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Logger.Errorf("unmarshal sqlite config failed: %v", err)
		return nil
	}
	return cfg
}

func (sp *SQLitePersistent) Init(cfgIn interface{}) error {
	if cfgIn == nil {
		cfgIn = LocalConfig{}
	}
	sp.Config = cfgIn.(LocalConfig)
	sp.Config.Normalize()

	cfgMap := map[string]interface{}{
		"format":      sp.Config.Format,
		"config_dir":  sp.Config.ConfigDir,
		"config_file": sp.Config.ConfigFile,
	}
	dialector, err := BuildDialector(cfgMap)
	if err != nil {
		return errors.Wrap(err, "")
	}
	logger := zapgorm2.New(log.ZapLog)
	logger.SetAsDefault()
	db, err := gorm.Open(dialector, &gorm.Config{Logger: logger})
	if err != nil {
		return errors.Wrap(err, "")
	}
	sp.Client = db
	sp.ParentDB = db

	if err := db.AutoMigrate(
		&TblCluster{},
		&TblLogic{},
		&TblQueryHistory{},
		&TblTask{},
		&TblBackup{},
		&TblBackupPolicy{},
		&TblBackupRun{},
		&TblMeta{},
		&TblUser{},
	); err != nil {
		return errors.Wrap(err, "")
	}

	if err := sp.migrateLegacyIfAny(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (sp *SQLitePersistent) Begin() error {
	if sp.Client != sp.ParentDB {
		return repository.ErrTransActionBegin
	}
	tx := sp.Client.Begin()
	sp.Client = tx
	return tx.Error
}

func (sp *SQLitePersistent) Commit() error {
	if sp.Client == sp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := sp.Client.Commit()
	sp.Client = sp.ParentDB
	return tx.Error
}

func (sp *SQLitePersistent) Rollback() error {
	if sp.Client == sp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := sp.Client.Rollback()
	sp.Client = sp.ParentDB
	return tx.Error
}

// wrapError 把 gorm 错误归一化到 repository.ErrRecordNotFound。沿用 mysql 后端模式。
func wrapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return repository.ErrRecordNotFound
	}
	return errors.Wrap(err, "")
}

// readMeta 读 TblMeta 中指定 key 的 value。不存在时返回 ("", nil)，不算错误。
func readMeta(db *gorm.DB, key string) (string, error) {
	// 用 Find + slice 而非 First：避免命中 ErrRecordNotFound 时 GORM logger 打 ERROR 噪音
	// （ckman 每次 fresh start 都会经历 not-found 路径，无意义的 ERROR 日志会困扰运维）。
	var ms []TblMeta
	if err := db.Where("key = ?", key).Limit(1).Find(&ms).Error; err != nil {
		return "", errors.Wrap(err, "")
	}
	if len(ms) == 0 {
		return "", nil
	}
	return ms[0].Value, nil
}

// writeMeta upsert 一条 KV 到 TblMeta。
func writeMeta(db *gorm.DB, key, value string) error {
	m := TblMeta{Key: key, Value: value}
	if err := db.Save(&m).Error; err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}


// ─── Cluster ──────────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) ClusterExists(cluster string) bool {
	_, err := sp.GetClusterbyName(cluster)
	return err == nil
}

func (sp *SQLitePersistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
	var tbl TblCluster
	if err := sp.Client.Where("cluster_name = ?", cluster).First(&tbl).Error; err != nil {
		return model.CKManClickHouseConfig{}, wrapError(err)
	}
	var conf model.CKManClickHouseConfig
	if err := json.Unmarshal([]byte(tbl.Config), &conf); err != nil {
		return model.CKManClickHouseConfig{}, errors.Wrap(err, "")
	}
	repository.DecodePasswd(&conf)
	return conf, nil
}

func (sp *SQLitePersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
	var tbls []TblCluster
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make(map[string]model.CKManClickHouseConfig, len(tbls))
	for _, tbl := range tbls {
		var conf model.CKManClickHouseConfig
		if err := json.Unmarshal([]byte(tbl.Config), &conf); err != nil {
			return nil, errors.Wrap(err, "")
		}
		repository.DecodePasswd(&conf)
		out[conf.Cluster] = conf
	}
	return out, nil
}

func (sp *SQLitePersistent) CreateCluster(conf model.CKManClickHouseConfig) error {
	repository.EncodePasswd(&conf)
	raw, err := json.Marshal(conf)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblCluster{ClusterName: conf.Cluster, Config: string(raw)}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateCluster(conf model.CKManClickHouseConfig) error {
	repository.EncodePasswd(&conf)
	raw, err := json.Marshal(conf)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := sp.Client.Model(&TblCluster{}).Where("cluster_name = ?", conf.Cluster).Updates(map[string]interface{}{
		"config": string(raw),
	})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteCluster(clusterName string) error {
	return wrapError(sp.Client.Where("cluster_name = ?", clusterName).Delete(&TblCluster{}).Error)
}

// ─── Logic Cluster ────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) GetLogicClusterbyName(logic string) ([]string, error) {
	var tbl TblLogic
	if err := sp.Client.Where("logic_name = ?", logic).First(&tbl).Error; err != nil {
		return nil, wrapError(err)
	}
	var physics []string
	if err := json.Unmarshal([]byte(tbl.PhysicClusters), &physics); err != nil {
		return nil, errors.Wrap(err, "")
	}
	return common.ArrayDistinct(physics), nil
}

func (sp *SQLitePersistent) GetAllLogicClusters() (map[string][]string, error) {
	var tbls []TblLogic
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make(map[string][]string, len(tbls))
	for _, tbl := range tbls {
		var physics []string
		if err := json.Unmarshal([]byte(tbl.PhysicClusters), &physics); err != nil {
			return nil, errors.Wrap(err, "")
		}
		out[tbl.LogicCluster] = common.ArrayDistinct(physics)
	}
	return out, nil
}

func (sp *SQLitePersistent) CreateLogicCluster(logic string, physics []string) error {
	raw, err := json.Marshal(common.ArrayDistinct(physics))
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblLogic{LogicCluster: logic, PhysicClusters: string(raw)}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateLogicCluster(logic string, physics []string) error {
	raw, err := json.Marshal(common.ArrayDistinct(physics))
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := sp.Client.Model(&TblLogic{}).Where("logic_name = ?", logic).Updates(map[string]interface{}{
		"physic_clusters": string(raw),
	})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteLogicCluster(clusterName string) error {
	return wrapError(sp.Client.Where("logic_name = ?", clusterName).Delete(&TblLogic{}).Error)
}

// ─── QueryHistory ─────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) GetAllQueryHistory() (map[string]model.QueryHistory, error) {
	var tbls []TblQueryHistory
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make(map[string]model.QueryHistory, len(tbls))
	for _, tbl := range tbls {
		out[tbl.CheckSum] = model.QueryHistory{
			CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
			QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
		}
	}
	return out, nil
}

func (sp *SQLitePersistent) GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error) {
	var tbls []TblQueryHistory
	if err := sp.Client.Where("cluster = ?", cluster).Order("create_time DESC").Limit(100).Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.QueryHistory, 0, len(tbls))
	for _, tbl := range tbls {
		out = append(out, model.QueryHistory{
			CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
			QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
		})
	}
	return out, nil
}

func (sp *SQLitePersistent) GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error) {
	var tbl TblQueryHistory
	if err := sp.Client.Where("checksum = ?", checksum).First(&tbl).Error; err != nil {
		return model.QueryHistory{}, wrapError(err)
	}
	return model.QueryHistory{
		CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
		QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
	}, nil
}

func (sp *SQLitePersistent) CreateQueryHistory(qh model.QueryHistory) error {
	qh.CreateTime = time.Now()
	tbl := TblQueryHistory{
		CheckSum: qh.CheckSum, Cluster: qh.Cluster,
		QuerySql: qh.QuerySql, CreateTime: qh.CreateTime,
	}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateQueryHistory(qh model.QueryHistory) error {
	qh.CreateTime = time.Now()
	tx := sp.Client.Model(&TblQueryHistory{}).Where("checksum = ?", qh.CheckSum).Updates(map[string]interface{}{
		"cluster":     qh.Cluster,
		"query":       qh.QuerySql,
		"create_time": qh.CreateTime,
	})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteQueryHistory(checksum string) error {
	tx := sp.Client.Where("checksum = ?", checksum).Delete(&TblQueryHistory{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) GetQueryHistoryCount(cluster string) int64 {
	var count int64
	sp.Client.Model(&TblQueryHistory{}).Where("cluster = ?", cluster).Count(&count)
	return count
}

func (sp *SQLitePersistent) GetEarliestQuery(cluster string) (model.QueryHistory, error) {
	var tbl TblQueryHistory
	if err := sp.Client.Where("cluster = ?", cluster).Order("create_time ASC").First(&tbl).Error; err != nil {
		return model.QueryHistory{}, wrapError(err)
	}
	return model.QueryHistory{
		CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
		QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
	}, nil
}

// ─── Task ─────────────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) CreateTask(task model.Task) error {
	task.CreateTime = time.Now()
	task.UpdateTime = task.CreateTime
	raw, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblTask{TaskId: task.TaskId, Status: task.Status, Task: string(raw)}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateTask(task model.Task) error {
	task.UpdateTime = time.Now()
	raw, err := json.Marshal(task)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := sp.Client.Model(&TblTask{}).Where("task_id = ?", task.TaskId).Updates(map[string]interface{}{
		"status": task.Status,
		"config": string(raw),
	})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteTask(id string) error {
	return wrapError(sp.Client.Where("task_id = ?", id).Delete(&TblTask{}).Error)
}

func (sp *SQLitePersistent) GetAllTasks() ([]model.Task, error) {
	var tbls []TblTask
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.Task, 0, len(tbls))
	for _, tbl := range tbls {
		var t model.Task
		if err := json.Unmarshal([]byte(tbl.Task), &t); err != nil {
			return nil, errors.Wrap(err, "")
		}
		out = append(out, t)
	}
	return out, nil
}

func (sp *SQLitePersistent) GetEffectiveTaskCount() int64 {
	var count int64
	sp.Client.Model(&TblTask{}).
		Where("status IN (?, ?)", model.TaskStatusRunning, model.TaskStatusWaiting).
		Count(&count)
	return count
}

func (sp *SQLitePersistent) GetPengdingTasks(serverIp string) ([]model.Task, error) {
	var tbls []TblTask
	if err := sp.Client.Where("status = ?", model.TaskStatusWaiting).Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.Task, 0, len(tbls))
	for _, tbl := range tbls {
		var task model.Task
		if err := json.Unmarshal([]byte(tbl.Task), &task); err != nil {
			return nil, errors.Wrap(err, "")
		}
		if task.ServerIp == serverIp {
			out = append(out, task)
		}
	}
	return out, nil
}

func (sp *SQLitePersistent) GetTaskbyTaskId(id string) (model.Task, error) {
	var tbl TblTask
	if err := sp.Client.Where("task_id = ?", id).First(&tbl).Error; err != nil {
		return model.Task{}, wrapError(err)
	}
	var task model.Task
	if err := json.Unmarshal([]byte(tbl.Task), &task); err != nil {
		return model.Task{}, errors.Wrap(err, "")
	}
	return task, nil
}

// ─── Backup ───────────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) CreateBackup(backup model.Backup) error {
	backup.CreateTime = time.Now()
	backup.UpdateTime = backup.CreateTime
	raw, err := json.Marshal(backup)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblBackup{
		BackupId: backup.BackupId, ClusterName: backup.ClusterName,
		UpdateTime: backup.UpdateTime.Format("2006-01-02 15:04:05.999"),
		Backup:     string(raw),
	}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateBackup(backup model.Backup) error {
	backup.UpdateTime = time.Now()
	raw, err := json.Marshal(backup)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := sp.Client.Model(&TblBackup{}).Where("backup_id = ?", backup.BackupId).Updates(map[string]interface{}{
		"cluster_name": backup.ClusterName,
		"update_time":  backup.UpdateTime.Format("2006-01-02 15:04:05.999"),
		"backup":       string(raw),
	})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteBackup(id string) error {
	return wrapError(sp.Client.Where("backup_id = ?", id).Delete(&TblBackup{}).Error)
}

func (sp *SQLitePersistent) GetAllBackups(cluster string) ([]model.Backup, error) {
	var tbls []TblBackup
	if err := sp.Client.Where("cluster_name = ?", cluster).Order("update_time DESC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.Backup, 0, len(tbls))
	for _, tbl := range tbls {
		var b model.Backup
		if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
			return nil, errors.Wrap(err, "")
		}
		out = append(out, b)
	}
	return out, nil
}

func (sp *SQLitePersistent) GetBackupById(id string) (model.Backup, error) {
	var tbl TblBackup
	if err := sp.Client.Where("backup_id = ?", id).First(&tbl).Error; err != nil {
		return model.Backup{}, wrapError(err)
	}
	var b model.Backup
	if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
		return model.Backup{}, errors.Wrap(err, "")
	}
	return b, nil
}

func (sp *SQLitePersistent) GetBackupByTable(cluster, database, table string) (model.Backup, error) {
	var tbls []TblBackup
	if err := sp.Client.Where("cluster_name = ?", cluster).Find(&tbls).Error; err != nil {
		return model.Backup{}, wrapError(err)
	}
	for _, tbl := range tbls {
		var b model.Backup
		if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
			return model.Backup{}, errors.Wrap(err, "")
		}
		if b.Database == database && b.Table == table {
			return b, nil
		}
	}
	return model.Backup{}, repository.ErrRecordNotFound
}

func (sp *SQLitePersistent) GetbackupByOperation(operation string) ([]model.Backup, error) {
	var tbls []TblBackup
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.Backup, 0)
	for _, tbl := range tbls {
		var b model.Backup
		if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
			return nil, errors.Wrap(err, "")
		}
		if b.Operation == operation {
			out = append(out, b)
		}
	}
	return out, nil
}

func (sp *SQLitePersistent) GetBackupByShechuleType(scheduleType string) ([]model.Backup, error) {
	var tbls []TblBackup
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.Backup, 0)
	for _, tbl := range tbls {
		var b model.Backup
		if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
			return nil, errors.Wrap(err, "")
		}
		if b.ScheduleType == scheduleType {
			out = append(out, b)
		}
	}
	return out, nil
}

// ─── BackupPolicy ─────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) CreateBackupPolicy(p model.BackupPolicy) error {
	if p.CreateTime.IsZero() {
		p.CreateTime = time.Now()
	}
	p.UpdateTime = time.Now()
	raw, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblBackupPolicy{
		PolicyID: p.PolicyID, ClusterName: p.ClusterName,
		Database: p.Database, Table: p.Table, Instance: p.Instance,
		ScheduleType: p.ScheduleType,
		Enabled:      p.Enabled, Deleted: false,
		Policy:     string(raw),
		UpdateTime: p.UpdateTime.Format("2006-01-02 15:04:05.999"),
	}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateBackupPolicy(p model.BackupPolicy) error {
	p.UpdateTime = time.Now()
	raw, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := sp.Client.Model(&TblBackupPolicy{}).Where("policy_id = ?", p.PolicyID).Updates(map[string]interface{}{
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
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteBackupPolicy(policyID string) error {
	tx := sp.Client.Model(&TblBackupPolicy{}).Where("policy_id = ?", policyID).Updates(map[string]interface{}{
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

func (sp *SQLitePersistent) GetBackupPolicy(policyID string) (model.BackupPolicy, error) {
	var tbl TblBackupPolicy
	if err := sp.Client.Where("policy_id = ?", policyID).First(&tbl).Error; err != nil {
		return model.BackupPolicy{}, wrapError(err)
	}
	var p model.BackupPolicy
	if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
		return model.BackupPolicy{}, errors.Wrap(err, "")
	}
	p.Deleted = tbl.Deleted
	p.Enabled = tbl.Enabled
	return p, nil
}

func (sp *SQLitePersistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	var tbls []TblBackupPolicy
	if err := sp.Client.Where("cluster_name = ? AND deleted = ?", cluster, false).Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
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

func (sp *SQLitePersistent) GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) {
	var tbls []TblBackupPolicy
	if err := sp.Client.Where("instance = ? AND enabled = ? AND schedule_type = ? AND deleted = ?",
		instance, true, model.BACKUP_SCHEDULED, false).Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
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

func (sp *SQLitePersistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
	var tbls []TblBackupPolicy
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
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

// ─── BackupRun ────────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) CreateBackupRun(r model.BackupRun) error {
	if r.CreateTime.IsZero() {
		r.CreateTime = time.Now()
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tbl := TblBackupRun{
		RunID: r.RunID, PolicyID: r.PolicyID,
		ClusterName: r.ClusterName, Database: r.Database, Table: r.Table,
		Status: r.Status, Instance: r.Instance,
		StartedAt:  r.StartedAt,
		Run:        string(raw),
		CreateTime: r.CreateTime,
	}
	return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateBackupRun(r model.BackupRun) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	tx := sp.Client.Model(&TblBackupRun{}).Where("run_id = ?", r.RunID).Updates(map[string]interface{}{
		"status":     r.Status,
		"instance":   r.Instance,
		"started_at": r.StartedAt,
		"run":        string(raw),
	})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteBackupRun(runID string) error {
	tx := sp.Client.Where("run_id = ?", runID).Delete(&TblBackupRun{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) GetBackupRun(runID string) (model.BackupRun, error) {
	var tbl TblBackupRun
	if err := sp.Client.Where("run_id = ?", runID).First(&tbl).Error; err != nil {
		return model.BackupRun{}, wrapError(err)
	}
	var r model.BackupRun
	if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
		return model.BackupRun{}, errors.Wrap(err, "")
	}
	return r, nil
}

func (sp *SQLitePersistent) GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
	q := sp.Client.Where("policy_id = ?", policyID)
	if !before.IsZero() {
		q = q.Where("started_at < ?", before)
	}
	q = q.Order("started_at DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	var tbls []TblBackupRun
	if err := q.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	return decodeBackupRuns(tbls)
}

func (sp *SQLitePersistent) GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error) {
	cutoff := time.Now().AddDate(0, 0, -sinceDays)
	var tbls []TblBackupRun
	if err := sp.Client.Where(
		"cluster_name = ? AND database_name = ? AND table_name = ? AND started_at > ?",
		cluster, database, table, cutoff,
	).Order("started_at DESC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	return decodeBackupRuns(tbls)
}

func (sp *SQLitePersistent) GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error) {
	var tbls []TblBackupRun
	if err := sp.Client.Where("policy_id = ? AND status IN (?, ?)",
		policyID, model.BACKUP_STATUS_QUEUED, model.BACKUP_STATUS_RUNNING).Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	return decodeBackupRuns(tbls)
}

func (sp *SQLitePersistent) GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error) {
	var tbls []TblBackupRun
	if err := sp.Client.Where("instance = ? AND status IN (?, ?)",
		instance, model.BACKUP_STATUS_QUEUED, model.BACKUP_STATUS_RUNNING).Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	return decodeBackupRuns(tbls)
}

func (sp *SQLitePersistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
	// First fetch the existing row to update its JSON blob atomically.
	var tbl TblBackupRun
	if err := sp.Client.Where("run_id = ? AND status = ?", runID, model.BACKUP_STATUS_QUEUED).
		First(&tbl).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Row not found or not in QUEUED state — CAS failed.
			return false, nil
		}
		return false, wrapError(err)
	}

	// Decode, mutate, re-encode.
	var r model.BackupRun
	if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
		return false, errors.Wrap(err, "")
	}
	r.Status = model.BACKUP_STATUS_RUNNING
	r.Instance = instance
	r.StartedAt = startedAt
	raw, err := json.Marshal(r)
	if err != nil {
		return false, errors.Wrap(err, "")
	}

	tx := sp.Client.Model(&TblBackupRun{}).
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

func (sp *SQLitePersistent) GetAllBackupRuns() ([]model.BackupRun, error) {
	var tbls []TblBackupRun
	if err := sp.Client.Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	return decodeBackupRuns(tbls)
}

func decodeBackupRuns(tbls []TblBackupRun) ([]model.BackupRun, error) {
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

func (sp *SQLitePersistent) UserExists(username string) bool {
	_, err := sp.GetUserByName(username)
	return err == nil
}

func (sp *SQLitePersistent) GetUserByName(username string) (model.CkmanUser, error) {
	var tbl TblUser
	if err := sp.Client.Where("username = ?", username).First(&tbl).Error; err != nil {
		return model.CkmanUser{}, wrapError(err)
	}
	return tblUserToModel(tbl), nil
}

func (sp *SQLitePersistent) GetAllUsers() ([]model.CkmanUser, error) {
	var tbls []TblUser
	if err := sp.Client.Order("username ASC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.CkmanUser, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, tblUserToModel(t))
	}
	return out, nil
}

func (sp *SQLitePersistent) CreateUser(u model.CkmanUser) error {
	tbl := TblUser{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Policy:       u.Policy,
		Enabled:      u.Enabled,
	}
	if err := sp.Client.Create(&tbl).Error; err != nil {
		if isUniqueViolation(err) {
			return repository.ErrRecordExists
		}
		return wrapError(err)
	}
	return nil
}

func (sp *SQLitePersistent) UpdateUser(u model.CkmanUser) error {
	updates := map[string]interface{}{
		"password_hash": u.PasswordHash,
		"policy":        u.Policy,
		"enabled":       u.Enabled,
	}
	tx := sp.Client.Model(&TblUser{}).Where("username = ?", u.Username).Updates(updates)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteUser(username string) error {
	tx := sp.Client.Where("username = ?", username).Delete(&TblUser{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// Compile-time guarantee that *SQLitePersistent satisfies PersistentMgr.
var _ repository.PersistentMgr = (*SQLitePersistent)(nil)
