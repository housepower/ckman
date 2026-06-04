package backup

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/housepower/ckman/model"
)

func TestInit_AssemblesRealExecutor(t *testing.T) {
	t.Skip("requires repository.Ps mock; verified by integration tests + manual run")
}

// ── claimRun / newRealExecutor（并发执行安全）─────────────────────────────────

func TestClaimRun_CASSemantics(t *testing.T) {
	// CAS 成功 → 执行
	ok := claimRun(func(runID, instance string, _ time.Time) (bool, error) {
		if runID != "r1" || instance != "self1" {
			t.Fatalf("unexpected args %s %s", runID, instance)
		}
		return true, nil
	}, "r1", "self1")
	if !ok {
		t.Fatal("claim should succeed when CAS returns true")
	}
	// run 已不是 queued（重复入队/已被认领）→ 跳过
	if claimRun(func(string, string, time.Time) (bool, error) { return false, nil }, "r1", "s") {
		t.Fatal("claim should fail when run is not queued")
	}
	// DB 故障 → 保守跳过
	if claimRun(func(string, string, time.Time) (bool, error) { return false, errors.New("db down") }, "r1", "s") {
		t.Fatal("claim should fail on repo error")
	}
}

func TestNewRealExecutor_InstancesAreIsolated(t *testing.T) {
	// 每个 run 必须拿到独立 Executor:run-scoped 的 conns/storage 不共享,
	// hooks 闭包绑定到各自实例。回归保护:防止改回共享单例。
	// 零值 adapter 即可：newRealExecutor 只取其方法值，不触发真实连接
	ch := &ClickHouseAdapter{}
	e1 := newRealExecutor(PersistentRepoAdapter{}, ch)
	e2 := newRealExecutor(PersistentRepoAdapter{}, ch)
	if e1 == e2 {
		t.Fatal("each call must return a fresh Executor")
	}
	e1.conns = []*shardConn{{host: "h1"}}
	e1.storage = &fakeStorage{}
	if len(e2.conns) != 0 || e2.storage != nil {
		t.Fatal("run-scoped state must not leak across executor instances")
	}
	// e2 的 execSQL 查的是 e2.conns(空),不应看到 e1 的 host
	if err := e2.execSQL("h1", "SELECT 1"); err == nil {
		t.Fatal("e2.execSQL must be bound to e2.conns, not e1's")
	}
}

func TestExecuteExclusive_PolicyMutex(t *testing.T) {
	repo := newMemRepo()
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_QUEUED}
	repo.runs["r2"] = model.BackupRun{RunID: "r2", PolicyID: "p1", Status: model.BACKUP_STATUS_QUEUED}
	var inFlight sync.Map
	claimOK := func(string) bool { return true }

	// p1 已有 run 在执行(预占锁) → r2 撞锁,标 skipped(overlap),不执行
	inFlight.Store("p1", "r1")
	executeExclusive(repo, &inFlight, "r2", claimOK, func(string) { t.Error("overlapped run must not execute") })
	r2, _ := repo.GetRun("r2")
	if r2.Status != model.BACKUP_STATUS_SKIPPED || r2.StatusReason != model.REASON_OVERLAP {
		t.Fatalf("expected skipped(overlap), got %s(%s)", r2.Status, r2.StatusReason)
	}

	// 锁释放后正常执行,执行完锁必须释放
	inFlight.Delete("p1")
	ran := false
	executeExclusive(repo, &inFlight, "r1", claimOK, func(string) { ran = true })
	if !ran {
		t.Fatal("run should execute when policy is free")
	}
	if _, loaded := inFlight.Load("p1"); loaded {
		t.Fatal("policy lock must be released after execution")
	}

	// claim 失败(已被认领/DB 故障)→ 不执行,且锁同样释放
	executeExclusive(repo, &inFlight, "r1", func(string) bool { return false }, func(string) { t.Error("must not execute when claim fails") })
	if _, loaded := inFlight.Load("p1"); loaded {
		t.Fatal("policy lock must be released after claim failure")
	}
}
