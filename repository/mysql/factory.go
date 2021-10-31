package mysql

import "github.com/housepower/ckman/repository"

func init() {
	repository.RegistePersistent(NewFactory)
}

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
	return NewMysqlPersistent()
}

func (factory *Factory) GetPersistentName() string {
	return MySQLPersistentName
}

func NewFactory() repository.PersistentFactory {
	return &Factory{}
}