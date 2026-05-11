package upgrade

import (
	"fmt"
	"testing"
	"time"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── fake persistent ─────────────────────────────────────────────────────────

type fakePersistent struct {
	clusters map[string]model.CKManClickHouseConfig
	backups  map[string][]model.Backup // cluster → rows
	policies []model.BackupPolicy
	runs     []model.BackupRun
	deleted  []string
}

func newFake() *fakePersistent {
	return &fakePersistent{
		clusters: make(map[string]model.CKManClickHouseConfig),
		backups:  make(map[string][]model.Backup),
	}
}

func (f *fakePersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
	return f.clusters, nil
}

func (f *fakePersistent) GetAllBackups(cluster string) ([]model.Backup, error) {
	return f.backups[cluster], nil
}

func (f *fakePersistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	var result []model.BackupPolicy
	for _, p := range f.policies {
		if p.ClusterName == cluster {
			result = append(result, p)
		}
	}
	return result, nil
}

func (f *fakePersistent) CreateBackupPolicy(p model.BackupPolicy) error {
	f.policies = append(f.policies, p)
	return nil
}

func (f *fakePersistent) CreateBackupRun(r model.BackupRun) error {
	f.runs = append(f.runs, r)
	return nil
}

func (f *fakePersistent) DeleteBackup(id string) error {
	f.deleted = append(f.deleted, id)
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func makeBackup(id, cluster, db, table, scheduleType, crontab, instance, operation, status string, parts []model.BackupLists) model.Backup {
	return model.Backup{
		BackupId:     id,
		ClusterName:  cluster,
		Database:     db,
		Table:        table,
		ScheduleType: scheduleType,
		Crontab:      crontab,
		Instance:     instance,
		Operation:    operation,
		Status:       status,
		Partitions:   parts,
		TargetType:   model.BACKUP_LOCAL,
		CreateTime:   time.Now().Add(-1 * time.Hour),
		UpdateTime:   time.Now(),
	}
}

func simpleParts(partition string) []model.BackupLists {
	return []model.BackupLists{
		{Partition: partition, Status: model.BACKUP_PARTITION_STATUS_SUCCESS, Size: 100, Rows: 10, FileNum: 1, Elapsed: 5},
	}
}

// ─── tests ───────────────────────────────────────────────────────────────────

// TestMigrate_GroupsByPolicyKey verifies that 3 old rows with the same
// (cluster, db, table, schedule_type, crontab, instance) produce exactly
// 1 BackupPolicy and 3 BackupRuns.
func TestMigrate_GroupsByPolicyKey(t *testing.T) {
	fp := newFake()
	fp.clusters["ckA"] = model.CKManClickHouseConfig{Cluster: "ckA"}
	fp.backups["ckA"] = []model.Backup{
		makeBackup("id-1", "ckA", "dba", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
		makeBackup("id-2", "ckA", "dba", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250502")),
		makeBackup("id-3", "ckA", "dba", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_FAILED, simpleParts("20250503")),
	}

	opts := BackupUpgradeOpts{Force: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 1, stats.PoliciesCreated, "should create exactly 1 policy")
	assert.Equal(t, 3, stats.RunsCreated, "should create 3 runs")
	assert.Equal(t, 0, stats.Skipped)

	require.Len(t, fp.policies, 1)
	assert.Equal(t, "ckA", fp.policies[0].ClusterName)
	assert.Equal(t, "dba", fp.policies[0].Database)
	assert.Equal(t, "t1", fp.policies[0].Table)
	assert.True(t, fp.policies[0].Enabled)
	assert.False(t, fp.policies[0].Deleted)

	require.Len(t, fp.runs, 3)
	runIDs := map[string]bool{}
	for _, r := range fp.runs {
		runIDs[r.RunID] = true
		assert.Equal(t, fp.policies[0].PolicyID, r.PolicyID)
		assert.Equal(t, model.TRIGGER_MIGRATED, r.TriggerType)
	}
	assert.True(t, runIDs["id-1"])
	assert.True(t, runIDs["id-2"])
	assert.True(t, runIDs["id-3"])
}

// TestMigrate_InfersBackupStyleFromPartitionsAll verifies BackupStyle inference:
// - Partitions=[{Partition:"all"}]  → BACKUP_STYLE_FULL
// - Partitions=[{Partition:"20250508"}] → BACKUP_STYLE_INCR
func TestMigrate_InfersBackupStyleFromPartitionsAll(t *testing.T) {
	fp := newFake()
	fp.clusters["ckB"] = model.CKManClickHouseConfig{Cluster: "ckB"}

	// Group 1: style=full (partition=="all")
	fp.backups["ckB"] = []model.Backup{
		makeBackup("full-1", "ckB", "db1", "t_full", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS,
			simpleParts("all")),
		// Group 2: style=incremental (specific partition name) — different table to force separate policy
		makeBackup("incr-1", "ckB", "db1", "t_incr", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS,
			simpleParts("20250508")),
	}

	opts := BackupUpgradeOpts{Force: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 2, stats.PoliciesCreated)
	assert.Equal(t, 2, stats.RunsCreated)

	policyByTable := make(map[string]model.BackupPolicy)
	for _, p := range fp.policies {
		policyByTable[p.Table] = p
	}

	fullPolicy, ok := policyByTable["t_full"]
	require.True(t, ok, "policy for t_full not found")
	assert.Equal(t, model.BACKUP_STYLE_FULL, fullPolicy.BackupStyle)

	incrPolicy, ok := policyByTable["t_incr"]
	require.True(t, ok, "policy for t_incr not found")
	assert.Equal(t, model.BACKUP_STYLE_INCR, incrPolicy.BackupStyle)
}

// TestMigrate_SkipsRowsWithEmptyPartitions verifies that rows with nil/empty
// Partitions are skipped and counted in Stats.Skipped, while other rows
// are still processed normally.
func TestMigrate_SkipsRowsWithEmptyPartitions(t *testing.T) {
	fp := newFake()
	fp.clusters["ckC"] = model.CKManClickHouseConfig{Cluster: "ckC"}

	good := makeBackup("good-1", "ckC", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS,
		simpleParts("20250501"))
	bad := makeBackup("bad-1", "ckC", "db1", "t2", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, nil)

	fp.backups["ckC"] = []model.Backup{good, bad}

	opts := BackupUpgradeOpts{Force: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err, "should not return error")
	assert.Equal(t, 1, stats.Skipped, "bad-1 should be skipped")
	assert.Equal(t, 1, stats.PoliciesCreated)
	assert.Equal(t, 1, stats.RunsCreated)
}

// TestMigrate_StatusMapping verifies that old Backup statuses map to the
// correct new BackupRun statuses.
func TestMigrate_StatusMapping(t *testing.T) {
	cases := []struct {
		oldStatus string
		newStatus string
	}{
		{model.BACKUP_STATUS_SUCCESS, model.BACKUP_STATUS_SUCCESS},
		{model.BACKUP_STATUS_FAILED, model.BACKUP_STATUS_FAILED},
		{model.BACKUP_STATUS_WAITING, model.BACKUP_STATUS_INTERRUPTED},
		{model.BACKUP_STATUS_INIT, model.BACKUP_STATUS_INTERRUPTED},
		{model.BACKUP_STATUS_PREPARE, model.BACKUP_STATUS_INTERRUPTED},
		{model.BACKUP_STATUS_BACKUP, model.BACKUP_STATUS_INTERRUPTED},
		{model.BACKUP_STATUS_RESTORE, model.BACKUP_STATUS_INTERRUPTED},
		{model.BACKUP_STATUS_CHECK, model.BACKUP_STATUS_INTERRUPTED},
		{model.BACKUP_STATUS_CLOSE, model.BACKUP_STATUS_INTERRUPTED},
		{"", model.BACKUP_STATUS_INTERRUPTED},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("old=%q", tc.oldStatus), func(t *testing.T) {
			got := mapStatus(tc.oldStatus)
			assert.Equal(t, tc.newStatus, got)
		})
	}
}

// TestMigrate_RestoreOpFindsBackupPolicy verifies that an operation=restore row
// is linked to the BackupPolicy created from the corresponding operation=backup rows.
func TestMigrate_RestoreOpFindsBackupPolicy(t *testing.T) {
	fp := newFake()
	fp.clusters["ckD"] = model.CKManClickHouseConfig{Cluster: "ckD"}

	backupRow := makeBackup("bk-1", "ckD", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1",
		model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501"))
	restoreRow := makeBackup("rs-1", "ckD", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1",
		model.OP_RESTORE, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501"))

	fp.backups["ckD"] = []model.Backup{backupRow, restoreRow}

	opts := BackupUpgradeOpts{Force: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 1, stats.PoliciesCreated, "1 policy from backup row")
	assert.Equal(t, 2, stats.RunsCreated, "1 backup run + 1 restore run")
	assert.Equal(t, 0, stats.LegacyPlaceholders, "no placeholder needed")

	require.Len(t, fp.policies, 1, "should have exactly 1 policy")
	policyID := fp.policies[0].PolicyID

	// Both runs should reference the same policy.
	require.Len(t, fp.runs, 2)
	for _, r := range fp.runs {
		assert.Equal(t, policyID, r.PolicyID, "run %s should reference policy %s", r.RunID, policyID)
	}

	// Verify operation fields.
	runByID := make(map[string]model.BackupRun)
	for _, r := range fp.runs {
		runByID[r.RunID] = r
	}
	assert.Equal(t, model.OP_BACKUP, runByID["bk-1"].Operation)
	assert.Equal(t, model.OP_RESTORE, runByID["rs-1"].Operation)
}

// TestMigrate_RestoreOpFallbackLegacyPolicy verifies that an operation=restore row
// with no corresponding backup policy gets a fallback placeholder policy (enabled=false).
func TestMigrate_RestoreOpFallbackLegacyPolicy(t *testing.T) {
	fp := newFake()
	fp.clusters["ckE"] = model.CKManClickHouseConfig{Cluster: "ckE"}

	// Only a restore row — no backup row for this table.
	restoreRow := makeBackup("rs-only", "ckE", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1",
		model.OP_RESTORE, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501"))
	fp.backups["ckE"] = []model.Backup{restoreRow}

	opts := BackupUpgradeOpts{Force: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 1, stats.LegacyPlaceholders, "should create 1 placeholder")
	assert.Equal(t, 1, stats.PoliciesCreated, "placeholder counts as created")
	assert.Equal(t, 1, stats.RunsCreated, "the restore run is created")
	assert.Equal(t, 0, stats.Skipped)

	require.Len(t, fp.policies, 1)
	placeholder := fp.policies[0]
	assert.False(t, placeholder.Enabled, "placeholder should be disabled")
	assert.Equal(t, "ckE", placeholder.ClusterName)
	assert.Equal(t, "db1", placeholder.Database)
	assert.Equal(t, "t1", placeholder.Table)

	require.Len(t, fp.runs, 1)
	assert.Equal(t, placeholder.PolicyID, fp.runs[0].PolicyID)
	assert.Equal(t, "rs-only", fp.runs[0].RunID)
}

// TestMigrate_CleanupDeletesRows verifies that --cleanup flag causes DeleteBackup
// to be called for each migrated row.
func TestMigrate_CleanupDeletesRows(t *testing.T) {
	fp := newFake()
	fp.clusters["ckF"] = model.CKManClickHouseConfig{Cluster: "ckF"}
	fp.backups["ckF"] = []model.Backup{
		makeBackup("del-1", "ckF", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
		makeBackup("del-2", "ckF", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250502")),
	}

	opts := BackupUpgradeOpts{Force: true, Cleanup: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 2, stats.CleanupRowsDeleted)
	assert.Len(t, fp.deleted, 2)
}

// TestMigrate_ForceCheckAborts verifies that without --force, the migration
// aborts if existing BackupPolicy rows are detected.
func TestMigrate_ForceCheckAborts(t *testing.T) {
	fp := newFake()
	fp.clusters["ckG"] = model.CKManClickHouseConfig{Cluster: "ckG"}
	// Pre-populate policies to simulate non-empty new table.
	fp.policies = []model.BackupPolicy{
		{PolicyID: "existing-1", ClusterName: "ckG"},
	}
	fp.backups["ckG"] = []model.Backup{
		makeBackup("bk-1", "ckG", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
	}

	opts := BackupUpgradeOpts{Force: false}
	_, err := runMigration(fp, opts)

	require.Error(t, err, "should abort when new tables are non-empty without --force")
	assert.Contains(t, err.Error(), "already has data")
}

// TestMigrate_DryRunWritesNothing verifies that dry-run mode produces Stats
// but never calls Create/Delete on the persistent layer.
func TestMigrate_DryRunWritesNothing(t *testing.T) {
	fp := newFake()
	fp.clusters["ckH"] = model.CKManClickHouseConfig{Cluster: "ckH"}
	fp.backups["ckH"] = []model.Backup{
		makeBackup("dry-1", "ckH", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
	}

	opts := BackupUpgradeOpts{Force: true, DryRun: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 0, len(fp.policies), "dry-run must not write policies")
	assert.Equal(t, 0, len(fp.runs), "dry-run must not write runs")
	// Stats are still computed.
	assert.Equal(t, 1, stats.PoliciesCreated)
	assert.Equal(t, 1, stats.RunsCreated)
}

// TestMigrate_EachPolicyGetsTaskIDEqualToPolicyID verifies that after migration
// every policy has a non-empty TaskID equal to its own PolicyID (self-contained task).
func TestMigrate_EachPolicyGetsTaskIDEqualToPolicyID(t *testing.T) {
	fp := newFake()
	fp.clusters["ckI"] = model.CKManClickHouseConfig{Cluster: "ckI"}
	// Two separate tables → two separate policies, each self-contained task.
	fp.backups["ckI"] = []model.Backup{
		makeBackup("bk-i1", "ckI", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
		makeBackup("bk-i2", "ckI", "db1", "t2", model.BACKUP_IMMEDIATE, "", "inst1", model.OP_BACKUP, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
	}

	opts := BackupUpgradeOpts{Force: true}
	stats, err := runMigration(fp, opts)

	require.NoError(t, err)
	assert.Equal(t, 2, stats.PoliciesCreated)

	require.Len(t, fp.policies, 2)
	for _, p := range fp.policies {
		assert.NotEmpty(t, p.TaskID, "migrated policy %s must have non-empty TaskID", p.PolicyID)
		assert.Equal(t, p.PolicyID, p.TaskID, "migrated policy %s TaskID must equal PolicyID (self-contained task)", p.PolicyID)
	}
}

// TestMigrate_PlaceholderPolicyGetsTaskID verifies that placeholder policies
// (for restore-only rows with no backup counterpart) also have TaskID = PolicyID.
func TestMigrate_PlaceholderPolicyGetsTaskID(t *testing.T) {
	fp := newFake()
	fp.clusters["ckJ"] = model.CKManClickHouseConfig{Cluster: "ckJ"}
	// Only a restore row — will create a placeholder policy.
	fp.backups["ckJ"] = []model.Backup{
		makeBackup("rs-j1", "ckJ", "db1", "t1", model.BACKUP_IMMEDIATE, "", "inst1",
			model.OP_RESTORE, model.BACKUP_STATUS_SUCCESS, simpleParts("20250501")),
	}

	opts := BackupUpgradeOpts{Force: true}
	_, err := runMigration(fp, opts)

	require.NoError(t, err)
	require.Len(t, fp.policies, 1)

	placeholder := fp.policies[0]
	assert.NotEmpty(t, placeholder.TaskID, "placeholder must have non-empty TaskID")
	assert.Equal(t, placeholder.PolicyID, placeholder.TaskID, "placeholder TaskID must equal PolicyID")
}
