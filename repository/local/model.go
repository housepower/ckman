package local

import (
	"github.com/housepower/ckman/model"
)

type PersistentData struct {
	Clusters     map[string]model.CKManClickHouseConfig `json:"clusters" yaml:"clusters"`
	Logics       map[string][]string                    `json:"logics" yaml:"logics"`
	QueryHistory map[string]model.QueryHistory          `json:"query_history" yaml:"query_history"`
	Task         map[string]model.Task                  `json:"tasks" yaml:"tasks"`
}

type Historys []model.QueryHistory

func CompareHistory(v1, v2 model.QueryHistory) bool {
	return v1.CreateTime.Before(v2.CreateTime)
}

func (v Historys) Len() int           { return len(v) }
func (v Historys) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v Historys) Less(i, j int) bool { return CompareHistory(v[i], v[j]) }
