package sqlite

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housepower/ckman/model"
)

func TestDiagnosisRunCRUDAndCAS(t *testing.T) {
	sp := newTestSP(t)
	now := time.Now().UnixMilli()
	run := model.DiagnosisRun{RunID: "diag-1", ClusterName: "c1", RequestedBy: "alice", Scene: "cluster_health",
		ChecksJSON: `["replica_status"]`, Status: model.DiagnosisStatusQueued, ReportJSON: strings.Repeat("x", 256*1024),
		CreatedAtMS: now, UpdatedAtMS: now, ExpiresAtMS: now + 10000}
	if err := sp.CreateDiagnosisRun(run); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wins := make(chan bool, 2)
	for _, instance := range []string{"a", "b"} {
		wg.Add(1)
		go func(instance string) {
			defer wg.Done()
			ok, err := sp.MarkDiagnosisRunRunningIfQueued(run.RunID, instance, now+1)
			if err != nil {
				t.Error(err)
			}
			wins <- ok
		}(instance)
	}
	wg.Wait()
	close(wins)
	winners := 0
	for ok := range wins {
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("claim winners=%d, want 1", winners)
	}

	got, err := sp.GetDiagnosisRun(run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.ReportJSON) != len(run.ReportJSON) || got.StartedAtMS != now+1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	list, err := sp.ListDiagnosisRuns(model.DiagnosisRunQuery{ClusterName: "c1", RequestedBy: "alice", Limit: 20})
	if err != nil || len(list) != 1 {
		t.Fatalf("list=%d err=%v", len(list), err)
	}
	ok, err := sp.RequestDiagnosisRunCancel(run.RunID, "bob", now+2)
	if err != nil || ok {
		t.Fatalf("other user canceled: ok=%v err=%v", ok, err)
	}
	ok, err = sp.RequestDiagnosisRunCancel(run.RunID, "alice", now+2)
	if err != nil || !ok {
		t.Fatalf("owner cancel: ok=%v err=%v", ok, err)
	}
	n, err := sp.MarkStaleDiagnosisRunsInterrupted(now+3, now+4)
	if err != nil || n != 1 {
		t.Fatalf("stale n=%d err=%v", n, err)
	}
	n, err = sp.DeleteExpiredDiagnosisRuns(now + 10001)
	if err != nil || n != 1 {
		t.Fatalf("expiry n=%d err=%v", n, err)
	}
}
