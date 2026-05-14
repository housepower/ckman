package cron

import (
	"os"
	"testing"

	"github.com/housepower/ckman/log"
	robfigcron "github.com/robfig/cron/v3"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func resetDynamicJobsForTest() {
	lock.Lock()
	defer lock.Unlock()
	DynamicJobs = map[string]Job{}
}

func TestAddJobAfterRemovePreservesEntryForUpdate(t *testing.T) {
	resetDynamicJobsForTest()
	defer resetDynamicJobsForTest()

	AddJob("p1", "0 3 10 * * ?", func() error { return nil })

	lock.Lock()
	j := DynamicJobs["p1"]
	j.status = JOB_SCHEDULED
	j.entryId = robfigcron.EntryID(7)
	DynamicJobs["p1"] = j
	lock.Unlock()

	RemoveJob("p1")
	AddJob("p1", "0 0 3 * * ?", func() error { return nil })

	lock.Lock()
	got := DynamicJobs["p1"]
	lock.Unlock()

	if got.op != JOB_UPD {
		t.Fatalf("expected JOB_UPD, got %d", got.op)
	}
	if got.entryId != robfigcron.EntryID(7) {
		t.Fatalf("expected old entry id to be preserved, got %d", got.entryId)
	}
	if got.spec != "0 0 3 * * ?" {
		t.Fatalf("expected new spec, got %s", got.spec)
	}
}

func TestAddJobSameScheduledSpecIsNoop(t *testing.T) {
	resetDynamicJobsForTest()
	defer resetDynamicJobsForTest()

	lock.Lock()
	DynamicJobs["p1"] = Job{
		op:      JOB_ADD,
		spec:    "0 0 3 * * ?",
		status:  JOB_SCHEDULED,
		entryId: robfigcron.EntryID(9),
	}
	lock.Unlock()

	AddJob("p1", "0 0 3 * * ?", func() error { return nil })

	lock.Lock()
	got := DynamicJobs["p1"]
	lock.Unlock()

	if got.op != JOB_ADD || got.status != JOB_SCHEDULED || got.entryId != robfigcron.EntryID(9) {
		t.Fatalf("same scheduled spec should not be replaced, got %+v", got)
	}
}
