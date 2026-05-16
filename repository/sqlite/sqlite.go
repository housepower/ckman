package sqlite

import (
	"encoding/json"
	"fmt"
	"time"

	sqlitedriver "github.com/glebarez/sqlite"
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

	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", sp.Config.DBPath())

	logger := zapgorm2.New(log.ZapLog)
	logger.SetAsDefault()
	db, err := gorm.Open(sqlitedriver.Open(dsn), &gorm.Config{Logger: logger})
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

// migrateLegacyIfAny is a TEMPORARY STUB for WU6.
// Real implementation lands in WU13 (Plan Task 18).
// For now: check _meta, mark fresh install if empty, else no-op.
func (sp *SQLitePersistent) migrateLegacyIfAny() error {
	cur, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
	if err != nil {
		return err
	}
	if cur != "" {
		return nil
	}
	return writeMeta(sp.Client, METAKEY_MIGRATED_FROM, META_FRESH_INSTALL)
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
