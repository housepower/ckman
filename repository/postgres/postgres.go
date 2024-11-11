package postgres

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
	driver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"moul.io/zapgorm2"
)

type PostgresPersistent struct {
	Config   PostgresConfig
	Client   *gorm.DB
	ParentDB *gorm.DB
}

func (mp *PostgresPersistent) Init(config interface{}) error {
	if config == nil {
		config = PostgresConfig{}
	}
	mp.Config = config.(PostgresConfig)
	mp.Config.Normalize()
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
		mp.Config.Host,
		mp.Config.Port,
		mp.Config.User,
		mp.Config.DataBase,
		mp.Config.Password)

	log.Logger.Debugf("postgres dsn:%s", dsn)
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

	//postgres automigrate have many bugs,so we create table with sql
	//err = mp.Client.AutoMigrate(
	//	&TblCluster{},
	//	&TblLogic{},
	//	&TblQueryHistory{},
	//	&TblTask{},
	//)
	//if err != nil {
	//	return err
	//}
	return nil
}

func (mp *PostgresPersistent) Begin() error {
	if mp.Client != mp.ParentDB {
		return repository.ErrTransActionBegin
	}
	tx := mp.Client.Begin()
	mp.Client = tx
	return tx.Error
}

func (mp *PostgresPersistent) Rollback() error {
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := mp.Client.Rollback()
	mp.Client = mp.ParentDB
	return tx.Error
}

func (mp *PostgresPersistent) Commit() error {
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := mp.Client.Commit()
	mp.Client = mp.ParentDB
	return tx.Error
}

func (mp *PostgresPersistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
	var config PostgresConfig
	data, err := json.Marshal(configMap)
	if err != nil {
		log.Logger.Errorf("marshal postgres configMap failed:%v", err)
		return nil
	}
	if err = json.Unmarshal(data, &config); err != nil {
		log.Logger.Errorf("unmarshal postgres config failed:%v", err)
		return nil
	}
	return config
}

func (mp *PostgresPersistent) ClusterExists(cluster string) bool {
	_, err := mp.GetClusterbyName(cluster)
	if err != nil {
		return false
	}
	return true
}

func (mp *PostgresPersistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
	var conf model.CKManClickHouseConfig
	var table TblCluster
	tx := mp.Client.Where("cluster_name = ?", cluster).First(&table)
	if tx.Error != nil {
		return model.CKManClickHouseConfig{}, wrapError(tx.Error)
	}
	if err := json.Unmarshal([]byte(table.Config), &conf); err != nil {
		return model.CKManClickHouseConfig{}, err
	}
	repository.DecodePasswd(&conf)
	return conf, nil
}

func (mp *PostgresPersistent) GetLogicClusterbyName(logic string) ([]string, error) {
	var table TblLogic
	tx := mp.Client.Where("logic_name = ?", logic).First(&table)
	if tx.Error != nil {
		return []string{}, wrapError(tx.Error)
	}
	physics := strings.Split(table.PhysicClusters, ",")
	return common.ArrayDistinct(physics), nil
}

func (mp *PostgresPersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
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

func (mp *PostgresPersistent) GetAllLogicClusters() (map[string][]string, error) {
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

func (mp *PostgresPersistent) CreateCluster(conf model.CKManClickHouseConfig) error {
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

func (mp *PostgresPersistent) CreateLogicCluster(logic string, physics []string) error {
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

func (mp *PostgresPersistent) UpdateCluster(conf model.CKManClickHouseConfig) error {
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

func (mp *PostgresPersistent) UpdateLogicCluster(logic string, physics []string) error {
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

func (mp *PostgresPersistent) DeleteCluster(clusterName string) error {
	tx := mp.Client.Where("cluster_name = ?", clusterName).Unscoped().Delete(&TblCluster{})
	return wrapError(tx.Error)
}

func (mp *PostgresPersistent) DeleteLogicCluster(clusterName string) error {
	tx := mp.Client.Where("logic_name = ?", clusterName).Unscoped().Delete(&TblLogic{})
	return wrapError(tx.Error)
}

func (mp *PostgresPersistent) GetAllQueryHistory() (map[string]model.QueryHistory, error) {
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

func (mp *PostgresPersistent) GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error) {
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

func (mp *PostgresPersistent) GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error) {
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

func (mp *PostgresPersistent) CreateQueryHistory(qh model.QueryHistory) error {
	table := TblQueryHistory{
		Cluster:    qh.Cluster,
		CheckSum:   qh.CheckSum,
		QuerySql:   qh.QuerySql,
		CreateTime: time.Now(),
	}
	tx := mp.Client.Create(&table)
	return wrapError(tx.Error)
}

func (mp *PostgresPersistent) UpdateQueryHistory(qh model.QueryHistory) error {
	table := TblQueryHistory{
		Cluster:    qh.Cluster,
		CheckSum:   qh.CheckSum,
		QuerySql:   qh.QuerySql,
		CreateTime: time.Now(),
	}
	tx := mp.Client.Model(TblQueryHistory{}).Where("checksum = ?", qh.CheckSum).Updates(&table)
	return wrapError(tx.Error)
}

func (mp *PostgresPersistent) DeleteQueryHistory(checksum string) error {
	tx := mp.Client.Where("checksum = ?", checksum).Unscoped().Delete(&TblQueryHistory{})
	return wrapError(tx.Error)
}

func (mp *PostgresPersistent) GetQueryHistoryCount() int64 {
	var count int64
	tx := mp.Client.Model(&TblQueryHistory{}).Count(&count)
	if tx.Error != nil {
		return 0
	}
	return count
}

func (mp *PostgresPersistent) GetEarliestQuery() (model.QueryHistory, error) {
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

func (mp *PostgresPersistent) CreateTask(task model.Task) error {
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

func (mp *PostgresPersistent) UpdateTask(task model.Task) error {
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

func (mp *PostgresPersistent) DeleteTask(id string) error {
	tx := mp.Client.Where("task_id = ?", id).Unscoped().Delete(&TblTask{})
	return wrapError(tx.Error)
}

func (mp *PostgresPersistent) GetAllTasks() ([]model.Task, error) {
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
			return []model.Task{}, errors.Wrap(err, "")
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (mp *PostgresPersistent) GetEffectiveTaskCount() int64 {
	var count int64
	tx := mp.Client.Model(&TblQueryHistory{}).Where("status in ?", []int{model.TaskStatusRunning, model.TaskStatusWaiting}).Count(&count)
	if tx.Error != nil {
		count = 0
	}
	return count
}

func (mp *PostgresPersistent) GetPengdingTasks(serverIp string) ([]model.Task, error) {
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

func (mp *PostgresPersistent) GetTaskbyTaskId(id string) (model.Task, error) {
	var table TblTask
	tx := mp.Client.Where("task_id = ?", id).First(&table)
	if tx.Error != nil {
		return model.Task{}, wrapError(tx.Error)
	}
	var task model.Task
	if err := json.Unmarshal([]byte(table.Task), &task); err != nil {
		return model.Task{}, err
	}

	return task, nil
}

func wrapError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = repository.ErrRecordNotFound
	}
	return err
}

func NewPostgresPersistent() *PostgresPersistent {
	return &PostgresPersistent{}
}
