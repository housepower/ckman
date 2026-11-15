package legacyjson

import (
	"sort"

	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

func (lp *LocalPersistent) persistDiagnosisLocked() {
	if !lp.InTransAction {
		_ = lp.dump()
	}
}

func (lp *LocalPersistent) CreateDiagnosisRun(r model.DiagnosisRun) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.DiagnosisRun[r.RunID]; ok {
		return repository.ErrRecordExists
	}
	lp.Data.DiagnosisRun[r.RunID] = r
	lp.persistDiagnosisLocked()
	return nil
}

func (lp *LocalPersistent) GetDiagnosisRun(id string) (model.DiagnosisRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	r, ok := lp.Data.DiagnosisRun[id]
	if !ok {
		return r, repository.ErrRecordNotFound
	}
	return r, nil
}

func (lp *LocalPersistent) ListDiagnosisRuns(q model.DiagnosisRunQuery) ([]model.DiagnosisRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	out := make([]model.DiagnosisRun, 0)
	for _, r := range lp.Data.DiagnosisRun {
		if (q.ClusterName != "" && r.ClusterName != q.ClusterName) ||
			(q.RequestedBy != "" && r.RequestedBy != q.RequestedBy) ||
			(q.Status != "" && r.Status != q.Status) || (q.Scene != "" && r.Scene != q.Scene) {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAtMS > out[j].CreatedAtMS })
	start := q.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(out) {
		return []model.DiagnosisRun{}, nil
	}
	limit := q.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	end := start + limit
	if end > len(out) {
		end = len(out)
	}
	return append([]model.DiagnosisRun(nil), out[start:end]...), nil
}

func (lp *LocalPersistent) UpdateDiagnosisRun(r model.DiagnosisRun) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.DiagnosisRun[r.RunID]; !ok {
		return repository.ErrRecordNotFound
	}
	lp.Data.DiagnosisRun[r.RunID] = r
	lp.persistDiagnosisLocked()
	return nil
}

func (lp *LocalPersistent) MarkDiagnosisRunRunningIfQueued(id, instance string, at int64) (bool, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	r, ok := lp.Data.DiagnosisRun[id]
	if !ok || r.Status != model.DiagnosisStatusQueued {
		return false, nil
	}
	r.Status = model.DiagnosisStatusRunning
	r.OwnerInstance = instance
	r.StartedAtMS = at
	r.UpdatedAtMS = at
	lp.Data.DiagnosisRun[id] = r
	lp.persistDiagnosisLocked()
	return true, nil
}

func (lp *LocalPersistent) RequestDiagnosisRunCancel(id, user string, at int64) (bool, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	r, ok := lp.Data.DiagnosisRun[id]
	if !ok || r.RequestedBy != user || (r.Status != model.DiagnosisStatusQueued && r.Status != model.DiagnosisStatusRunning) {
		return false, nil
	}
	r.CancelRequested = true
	r.Status = model.DiagnosisStatusCanceling
	r.UpdatedAtMS = at
	lp.Data.DiagnosisRun[id] = r
	lp.persistDiagnosisLocked()
	return true, nil
}

func (lp *LocalPersistent) HeartbeatDiagnosisRun(id, instance string, at int64) (bool, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	r, ok := lp.Data.DiagnosisRun[id]
	if !ok || r.OwnerInstance != instance || (r.Status != model.DiagnosisStatusRunning && r.Status != model.DiagnosisStatusCanceling) {
		return false, nil
	}
	r.UpdatedAtMS = at
	lp.Data.DiagnosisRun[id] = r
	lp.persistDiagnosisLocked()
	return true, nil
}

func (lp *LocalPersistent) MarkStaleDiagnosisRunsInterrupted(before, at int64) (int64, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	var n int64
	for id, r := range lp.Data.DiagnosisRun {
		if (r.Status == model.DiagnosisStatusRunning || r.Status == model.DiagnosisStatusCanceling) && r.UpdatedAtMS < before {
			r.Status = model.DiagnosisStatusInterrupted
			r.FinishedAtMS = at
			r.UpdatedAtMS = at
			lp.Data.DiagnosisRun[id] = r
			n++
		}
	}
	lp.persistDiagnosisLocked()
	return n, nil
}

func (lp *LocalPersistent) DeleteExpiredDiagnosisRuns(at int64) (int64, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	var n int64
	for id, r := range lp.Data.DiagnosisRun {
		if r.ExpiresAtMS > 0 && r.ExpiresAtMS < at {
			delete(lp.Data.DiagnosisRun, id)
			n++
		}
	}
	lp.persistDiagnosisLocked()
	return n, nil
}
