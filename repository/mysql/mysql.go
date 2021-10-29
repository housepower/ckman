package mysql

import (
	"encoding/json"
	"fmt"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strings"
	"time"
)

type MysqlPersistent struct {
	Config   MysqlConfig
	Client   *gorm.DB
	ParentDB *gorm.DB
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
	db, err := gorm.Open(driver.Open(dsn), &gorm.Config{})
	if err != nil {

		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
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
	return mp.Client.AutoMigrate(&TblCluster{}, &TblLogic{})
}

func (mp *MysqlPersistent) Begin() error {
	if mp.Client != mp.ParentDB {
		return repository.ErrTransActionBegin
	}
	tx := mp.Client.Begin()
	mp.Client = tx
	return tx.Error
}

func (mp *MysqlPersistent) Rollback() error{
	if mp.Client == mp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx :=  mp.Client.Rollback()
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

func (mp *MysqlPersistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
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

func (mp *MysqlPersistent) GetLogicClusterbyName(logic string) ([]string, error) {
	var table TblLogic
	tx := mp.Client.Where("logic_name = ?", logic).First(&table)
	if tx.Error != nil {
		return []string{}, wrapError(tx.Error)
	}
	physics := strings.Split(table.PhysicClusters, ",")
	return physics, nil
}

func (mp *MysqlPersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
	var tables []TblCluster
	clusterMapping := make(map[string]model.CKManClickHouseConfig)
	tx := mp.Client.Find(&tables)
	if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
		return nil, tx.Error
	}
	for _, table := range tables {
		var conf model.CKManClickHouseConfig
		if err := json.Unmarshal([]byte(table.Config), &conf); err != nil {
			return nil, err
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
		return nil, tx.Error
	}
	for _, table := range tables {
		physics := strings.Split(table.PhysicClusters, ",")
		logicMapping[table.LogicCluster] = physics
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
		return err
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
		PhysicClusters: strings.Join(physics, ","),
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
		return err
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
		PhysicClusters: strings.Join(physics, ","),
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

func wrapError(err error) error {
	if err == gorm.ErrRecordNotFound {
		err = repository.ErrRecordNotFound
	}
	return err
}

func NewMysqlPersistent() *MysqlPersistent {
	return &MysqlPersistent{}
}
