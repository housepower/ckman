package local

import "github.com/housepower/ckman/model"

type PersistentData struct {
	Clusters map[string]model.CKManClickHouseConfig `json:"clusters" yaml:"clusters"`
	Logics   map[string][]string                    `json:"logics" yaml:"logics"`
}