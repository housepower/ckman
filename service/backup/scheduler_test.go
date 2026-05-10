package backup

import (
	"os"
	"sync"
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

type fakeCron struct {
	mu   sync.Mutex
	jobs map[string]string // policy_id -> crontab
}

func newFakeCron() *fakeCron { return &fakeCron{jobs: map[string]string{}} }
func (f *fakeCron) Add(id, spec string, _ func() error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[id] = spec
}
func (f *fakeCron) Remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.jobs, id)
}
func (f *fakeCron) snapshot() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k, v := range f.jobs {
		out[k] = v
	}
	return out
}

type fakePolicyRepo struct{ items []model.BackupPolicy }

func (r *fakePolicyRepo) Active(_ string) []model.BackupPolicy { return r.items }

func TestScheduler_Reconcile_Add(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 3 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}}
	cr := newFakeCron()
	s := NewScheduler("self", cr, repo, func(model.BackupPolicy) {})
	s.Reconcile()
	got := cr.snapshot()
	if len(got) != 1 || got["p1"] != "0 3 * * *" {
		t.Fatalf("expected p1=0 3 * * *, got %v", got)
	}
}

func TestScheduler_Reconcile_Remove(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{}}
	cr := newFakeCron()
	cr.jobs["p1"] = "0 3 * * *"
	s := NewScheduler("self", cr, repo, func(model.BackupPolicy) {})
	// simulate that a previous reconcile already tracked p1
	s.tracked["p1"] = "0 3 * * *"
	s.Reconcile()
	if _, ok := cr.snapshot()["p1"]; ok {
		t.Fatal("p1 should be removed")
	}
}

func TestScheduler_Reconcile_CrontabChange(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 5 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}}
	cr := newFakeCron()
	cr.jobs["p1"] = "0 3 * * *"
	s := NewScheduler("self", cr, repo, func(model.BackupPolicy) {})
	s.tracked["p1"] = "0 3 * * *"
	s.Reconcile()
	got := cr.snapshot()
	if got["p1"] != "0 5 * * *" {
		t.Fatalf("expected updated crontab, got %v", got)
	}
}

func TestScheduler_Reconcile_StableNoOp(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 3 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}}
	cr := &fakeCronCounting{fakeCron: newFakeCron()}
	cr.jobs["p1"] = "0 3 * * *"
	s := NewScheduler("self", cr, repo, func(model.BackupPolicy) {})
	s.tracked["p1"] = "0 3 * * *"
	s.Reconcile()
	if cr.addCount != 0 {
		t.Fatalf("stable reconcile should not Add: addCount=%d", cr.addCount)
	}
	if cr.removeCount != 0 {
		t.Fatalf("stable reconcile should not Remove: removeCount=%d", cr.removeCount)
	}
}

type fakeCronCounting struct {
	*fakeCron
	addCount    int
	removeCount int
}

func (f *fakeCronCounting) Add(id, spec string, fn func() error) {
	f.addCount++
	f.fakeCron.Add(id, spec, fn)
}

func (f *fakeCronCounting) Remove(id string) {
	f.removeCount++
	f.fakeCron.Remove(id)
}

func TestScheduler_Reconcile_FnInvokedWithCorrectPolicy(t *testing.T) {
	// closure captures the correct policy (no loop variable bug)
	policies := []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 3 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
		{PolicyID: "p2", Crontab: "0 4 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}
	repo := &fakePolicyRepo{items: policies}
	cr := newFakeCron()
	capturedFns := map[string]func() error{}
	wrap := &fakeCronCapture{fakeCron: cr, captured: capturedFns}
	got := []string{}
	fn := func(p model.BackupPolicy) { got = append(got, p.PolicyID) }
	s := NewScheduler("self", wrap, repo, fn)
	s.Reconcile()
	// trigger both captured fns
	for _, f := range capturedFns {
		_ = f()
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 invocations, got %d: %v", len(got), got)
	}
	// both p1 and p2 must be triggered (not p2 twice — loop variable bug)
	seen := map[string]bool{got[0]: true, got[1]: true}
	if !seen["p1"] || !seen["p2"] {
		t.Fatalf("expected both p1 and p2, got %v", got)
	}
}

type fakeCronCapture struct {
	*fakeCron
	captured map[string]func() error
}

func (f *fakeCronCapture) Add(id, spec string, fn func() error) {
	f.captured[id] = fn
	f.fakeCron.Add(id, spec, fn)
}
