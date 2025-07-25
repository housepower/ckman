package dm8

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
	driver "github.com/wanlay/gorm-dm8"
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
	dsn := fmt.Sprintf("dm://%s:%s@%s:%d?autoCommit=true",
		mp.Config.User,
		mp.Config.Password,
		mp.Config.Host,
		mp.Config.Port)

	log.Logger.Debugf("DM8 dsn:%s", dsn)
	logger := zapgorm2.New(log.ZapLog)
	logger.SetAsDefault()
	db, err := gorm.Open(driver.Open(dsn), &gorm.Config{
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
	tx := mp.Client.Where("cluster = ?", cluster).Order("create_time DESC").Limit(100).Find(&tables)
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
	tx := mp.Client.Model(&TblQueryHistory{}).Where("cluster = ?", cluster).Count(&count)
	if tx.Error != nil {
		return 0
	}
	return count
}

func (mp *DM8Persistent) GetEarliestQuery(cluster string) (model.QueryHistory, error) {
	var table TblQueryHistory
	tx := mp.Client.Where("cluster = ?", cluster).Order("create_time").First(&table)
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
	tx := mp.Client.Where("cluster_name =?", cluster).Order("update_time").Find(&tbackups)
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

func wrapError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = repository.ErrRecordNotFound
	}
	return err
}

func NewDM8Persistent() *DM8Persistent {
	return &DM8Persistent{}
}
