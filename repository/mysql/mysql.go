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

func (mp *MysqlPersistent) GetQueryHistoryCount() int64 {
	var count int64
	tx := mp.Client.Model(&TblQueryHistory{}).Count(&count)
	if tx.Error != nil {
		return 0
	}
	return count
}

func (mp *MysqlPersistent) GetEarliestQuery() (model.QueryHistory, error) {
	var table TblQueryHistory
	tx := mp.Client.Order("create_time").First(&table)
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
	tx := mp.Client.Model(&TblQueryHistory{}).Where("status in ?", []int{model.TaskStatusRunning, model.TaskStatusWaiting}).Count(&count)
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

func wrapError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = repository.ErrRecordNotFound
	}
	return err
}

func NewMysqlPersistent() *MysqlPersistent {
	return &MysqlPersistent{}
}
