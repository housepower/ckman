package postgres

import "github.com/housepower/ckman/repository"

func init() {
	repository.RegistePersistent(NewFactory)
}

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
	return NewPostgresPersistent()
}

func (factory *Factory) GetPersistentName() string {
	return PostgresPersistentName
}

func NewFactory() repository.PersistentFactory {
	return &Factory{}
}