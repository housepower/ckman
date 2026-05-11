package backup

import (
	"errors"
	"testing"
	"time"

	"github.com/housepower/ckman/model"
)

type memRepo struct {
	policies map[string]model.BackupPolicy
	runs     map[string]model.BackupRun
}

func newMemRepo() *memRepo {
	return &memRepo{policies: map[string]model.BackupPolicy{}, runs: map[string]model.BackupRun{}}
}

func (r *memRepo) CreatePolicy(p model.BackupPolicy) error    { r.policies[p.PolicyID] = p; return nil }
func (r *memRepo) UpdatePolicy(p model.BackupPolicy) error    { r.policies[p.PolicyID] = p; return nil }
func (r *memRepo) ListPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	var out []model.BackupPolicy
	for _, p := range r.policies {
		if p.ClusterName == cluster {
			out = append(out, p)
		}
	}
	return out, nil
}
func (r *memRepo) GetPolicy(id string) (model.BackupPolicy, error) {
	p, ok := r.policies[id]
	if !ok {
		return model.BackupPolicy{}, errors.New("not found")
	}
	return p, nil
}
func (r *memRepo) CreateRun(rn model.BackupRun) error { r.runs[rn.RunID] = rn; return nil }
func (r *memRepo) UpdateRun(rn model.BackupRun) error { r.runs[rn.RunID] = rn; return nil }
func (r *memRepo) GetRun(id string) (model.BackupRun, error) {
	rn, ok := r.runs[id]
	if !ok {
		return model.BackupRun{}, errors.New("not found")
	}
	return rn, nil
}
func (r *memRepo) InFlightRunsByPolicy(policyID string) []model.BackupRun {
	var out []model.BackupRun
	for _, rn := range r.runs {
		if rn.PolicyID == policyID && (rn.Status == model.BACKUP_STATUS_QUEUED || rn.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, rn)
		}
	}
	return out
}

type fakePool struct {
	full bool
	in   []string
}

func (f *fakePool) Submit(id string) bool {
	if f.full {
		return false
	}
	f.in = append(f.in, id)
	return true
}

func TestService_Submit_NormalCase(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)
	policy := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_IMMEDIATE, Instance: "ckman-01", Enabled: false,
	}
	repo.policies["p1"] = policy
	runID, err := svc.SubmitForPolicy(policy, model.TRIGGER_MANUAL_IMMEDIATE)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if runID == "" {
		t.Fatal("empty runID")
	}
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_QUEUED {
		t.Fatalf("run not queued: %+v", rn)
	}
	if len(pool.in) != 1 || pool.in[0] != runID {
		t.Fatalf("pool not enqueued: %+v", pool.in)
	}
}

func TestService_Submit_OverlapWritesSkipped(t *testing.T) {
	repo := newMemRepo()
	policy := model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Instance: "ckman-01", Enabled: true}
	repo.policies["p1"] = policy
	repo.runs["r-prev"] = model.BackupRun{
		RunID: "r-prev", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING,
	}
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	runID, err := svc.SubmitForPolicy(policy, model.TRIGGER_CRON)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_SKIPPED || rn.StatusReason != model.REASON_OVERLAP {
		t.Fatalf("expected skipped(overlap), got %+v", rn)
	}
	if len(pool.in) != 0 {
		t.Fatal("pool should not receive overlap-skipped run")
	}
}

func TestService_Submit_QueueFullWritesSkipped(t *testing.T) {
	repo := newMemRepo()
	policy := model.BackupPolicy{PolicyID: "p1", Instance: "ckman-01", ScheduleType: model.BACKUP_SCHEDULED, Enabled: true}
	repo.policies["p1"] = policy
	pool := &fakePool{full: true}
	svc := newServiceForTest("ckman-01", repo, pool)

	runID, err := svc.SubmitForPolicy(policy, model.TRIGGER_CRON)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_SKIPPED || rn.StatusReason != model.REASON_QUEUE_FULL {
		t.Fatalf("expected skipped(queue_full), got %+v", rn)
	}
}

func TestService_Submit_DisabledPolicyWritesSkipped(t *testing.T) {
	repo := newMemRepo()
	policy := model.BackupPolicy{PolicyID: "p1", Instance: "ckman-01",
		ScheduleType: model.BACKUP_SCHEDULED, Enabled: false}
	repo.policies["p1"] = policy
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	runID, _ := svc.SubmitForPolicy(policy, model.TRIGGER_CRON)
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_SKIPPED || rn.StatusReason != model.REASON_DISABLED {
		t.Fatalf("expected skipped(disabled), got %+v", rn)
	}
	if len(pool.in) != 0 {
		t.Fatal("disabled run should not enqueue")
	}
}

func newServiceForTest(self string, repo ServiceRepo, pool ServicePool) *Service {
	s := NewService(self, repo, pool)
	// 测试用固定 now 让 timestamp 可预期（可选）
	s.now = func() time.Time { return time.Unix(1, 0) }
	return s
}

func (r *memRepo) InFlightRunsByInstance(instance string) []model.BackupRun {
	var out []model.BackupRun
	for _, rn := range r.runs {
		if rn.Instance == instance && (rn.Status == model.BACKUP_STATUS_QUEUED || rn.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, rn)
		}
	}
	return out
}

// Task 6 追加：GetRun delegates to repo
func TestService_GetRun_Delegates(t *testing.T) {
	repo := newMemRepo()
	expected := model.BackupRun{RunID: "r-abc", PolicyID: "p1", Status: model.BACKUP_STATUS_SUCCESS}
	repo.runs["r-abc"] = expected

	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	got, err := svc.GetRun("r-abc")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.RunID != expected.RunID || got.Status != expected.Status {
		t.Fatalf("unexpected run: %+v", got)
	}
}

// Task 14 追加：Boot 把本实例残留 in-flight run 标 interrupted
func TestService_Boot_MarksInFlightAsInterrupted(t *testing.T) {
	repo := newMemRepo()
	repo.runs["r-running"] = model.BackupRun{RunID: "r-running", Status: model.BACKUP_STATUS_RUNNING, Instance: "ckman-01"}
	repo.runs["r-queued"] = model.BackupRun{RunID: "r-queued", Status: model.BACKUP_STATUS_QUEUED, Instance: "ckman-01"}
	repo.runs["r-other"] = model.BackupRun{RunID: "r-other", Status: model.BACKUP_STATUS_RUNNING, Instance: "ckman-02"}
	repo.runs["r-success"] = model.BackupRun{RunID: "r-success", Status: model.BACKUP_STATUS_SUCCESS, Instance: "ckman-01"}

	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	if err := svc.Boot(); err != nil {
		t.Fatalf("boot: %v", err)
	}
	r1, _ := repo.GetRun("r-running")
	if r1.Status != model.BACKUP_STATUS_INTERRUPTED {
		t.Fatalf("running should be interrupted: %+v", r1)
	}
	if r1.StatusReason != model.REASON_RESTART {
		t.Fatalf("status_reason should be ckman restart: %+v", r1)
	}
	r2, _ := repo.GetRun("r-queued")
	if r2.Status != model.BACKUP_STATUS_INTERRUPTED {
		t.Fatalf("queued should be interrupted: %+v", r2)
	}
	r3, _ := repo.GetRun("r-other")
	if r3.Status != model.BACKUP_STATUS_RUNNING {
		t.Fatalf("other instance unaffected: %+v", r3)
	}
	r4, _ := repo.GetRun("r-success")
	if r4.Status != model.BACKUP_STATUS_SUCCESS {
		t.Fatalf("success terminal state must not change: %+v", r4)
	}
}
