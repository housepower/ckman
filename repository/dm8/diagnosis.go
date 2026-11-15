package dm8

import (
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

func (mp *DM8Persistent) CreateDiagnosisRun(r model.DiagnosisRun) error {
	return repository.CreateDiagnosisRun(mp.Client, r)
}
func (mp *DM8Persistent) GetDiagnosisRun(id string) (model.DiagnosisRun, error) {
	return repository.GetDiagnosisRun(mp.Client, id)
}
func (mp *DM8Persistent) ListDiagnosisRuns(q model.DiagnosisRunQuery) ([]model.DiagnosisRun, error) {
	return repository.ListDiagnosisRuns(mp.Client, q)
}
func (mp *DM8Persistent) UpdateDiagnosisRun(r model.DiagnosisRun) error {
	return repository.UpdateDiagnosisRun(mp.Client, r)
}
func (mp *DM8Persistent) MarkDiagnosisRunRunningIfQueued(id, instance string, at int64) (bool, error) {
	return repository.MarkDiagnosisRunRunningIfQueued(mp.Client, id, instance, at)
}
func (mp *DM8Persistent) RequestDiagnosisRunCancel(id, user string, at int64) (bool, error) {
	return repository.RequestDiagnosisRunCancel(mp.Client, id, user, at)
}
func (mp *DM8Persistent) HeartbeatDiagnosisRun(id, instance string, at int64) (bool, error) {
	return repository.HeartbeatDiagnosisRun(mp.Client, id, instance, at)
}
func (mp *DM8Persistent) MarkStaleDiagnosisRunsInterrupted(before, at int64) (int64, error) {
	return repository.MarkStaleDiagnosisRunsInterrupted(mp.Client, before, at)
}
func (mp *DM8Persistent) DeleteExpiredDiagnosisRuns(at int64) (int64, error) {
	return repository.DeleteExpiredDiagnosisRuns(mp.Client, at)
}
