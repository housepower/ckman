package sqlite

import "github.com/housepower/ckman/repository"

func init() {
	repository.RegistePersistent(NewFactory)
}

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
	return NewSQLitePersistent()
}

func (factory *Factory) GetPersistentName() string {
	return SQLitePersistentName
}

func NewFactory() repository.PersistentFactory {
	return &Factory{}
}
