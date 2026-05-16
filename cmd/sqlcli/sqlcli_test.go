package sqlcli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/housepower/ckman/log"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func TestOpenDB_SQLite(t *testing.T) {
	dir := t.TempDir()
	cfgMap := map[string]interface{}{
		"config_dir":  dir,
		"config_file": "testdb",
	}
	db, backend, err := OpenDB("local", cfgMap)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if backend != BackendSQLite {
		t.Fatalf("unexpected backend: %v", backend)
	}
	if db == nil {
		t.Fatalf("nil db")
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()
}

func TestOpenDB_UnsupportedPolicy(t *testing.T) {
	_, _, err := OpenDB("oracle", map[string]interface{}{})
	if err == nil {
		t.Fatalf("expected error for unsupported policy")
	}
}

func TestTranslate_ShowDatabases(t *testing.T) {
	cases := []struct {
		backend Backend
		want    string
	}{
		{BackendSQLite, "PRAGMA database_list"},
		{BackendMySQL, "SHOW DATABASES"},
		{BackendDM8, "SHOW DATABASES"},
		{BackendPostgres, "SELECT datname FROM pg_database WHERE datistemplate = false"},
	}
	for _, c := range cases {
		got := Translate("SHOW DATABASES", c.backend)
		if got != c.want {
			t.Errorf("%v: got %q, want %q", c.backend, got, c.want)
		}
	}
}

func TestTranslate_ShowTables(t *testing.T) {
	cases := []struct {
		backend Backend
		want    string
	}{
		{BackendSQLite, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"},
		{BackendMySQL, "SHOW TABLES"},
		{BackendDM8, "SHOW TABLES"},
		{BackendPostgres, "SELECT tablename FROM pg_tables WHERE schemaname='public'"},
	}
	for _, c := range cases {
		got := Translate("show tables", c.backend) // case-insensitive
		if got != c.want {
			t.Errorf("%v: got %q, want %q", c.backend, got, c.want)
		}
	}
}

func TestTranslate_Desc(t *testing.T) {
	cases := []struct {
		input   string
		backend Backend
		want    string
	}{
		{"DESC tbl_cluster", BackendSQLite, "PRAGMA table_info(tbl_cluster)"},
		{"DESCRIBE tbl_cluster", BackendSQLite, "PRAGMA table_info(tbl_cluster)"},
		{"DESC tbl_cluster", BackendMySQL, "DESC tbl_cluster"},
		{"DESC tbl_cluster", BackendPostgres,
			"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl_cluster'"},
	}
	for _, c := range cases {
		got := Translate(c.input, c.backend)
		if got != c.want {
			t.Errorf("%s on %v: got %q, want %q", c.input, c.backend, got, c.want)
		}
	}
}

func TestTranslate_PassThrough(t *testing.T) {
	in := "SELECT * FROM tbl_cluster WHERE cluster_name = 'ck1'"
	if got := Translate(in, BackendSQLite); got != in {
		t.Errorf("expected pass-through, got %q", got)
	}
}

func sampleRows() ([]string, [][]interface{}) {
	cols := []string{"id", "name", "comment", "created_at"}
	rows := [][]interface{}{
		{int64(1), "ck1", "prod", time.Date(2026, 5, 16, 10, 30, 45, 0, time.UTC)},
		{int64(2), "ck2", nil, time.Date(2026, 5, 16, 11, 2, 11, 0, time.UTC)},
	}
	return cols, rows
}

func TestRender_Table_Basic(t *testing.T) {
	cols, rows := sampleRows()
	var buf bytes.Buffer
	if err := RenderTable(&buf, cols, rows, RenderOptions{}); err != nil {
		t.Fatalf("RenderTable: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"id", "name", "comment", "ck1", "ck2", "(NULL)"} {
		if !strings.Contains(out, want) {
			t.Errorf("table missing %q in:\n%s", want, out)
		}
	}
}

func TestRender_Table_Truncate(t *testing.T) {
	cols := []string{"v"}
	longStr := strings.Repeat("x", 100)
	rows := [][]interface{}{{longStr}}
	var buf bytes.Buffer
	if err := RenderTable(&buf, cols, rows, RenderOptions{TruncateAt: 60}); err != nil {
		t.Fatalf("RenderTable: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, longStr) {
		t.Errorf("expected truncation, got full string in:\n%s", out)
	}
	if !strings.Contains(out, "...") {
		t.Errorf("expected ellipsis marker")
	}
}

func TestRender_Vertical_Basic(t *testing.T) {
	cols, rows := sampleRows()
	var buf bytes.Buffer
	if err := RenderVertical(&buf, cols, rows, RenderOptions{}); err != nil {
		t.Fatalf("RenderVertical: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "1. row") || !strings.Contains(out, "2. row") {
		t.Errorf("vertical missing row markers:\n%s", out)
	}
	if !strings.Contains(out, "id:") || !strings.Contains(out, "name:") {
		t.Errorf("vertical missing field labels")
	}
}

func TestRender_JSON_NullAndTime(t *testing.T) {
	cols, rows := sampleRows()
	var buf bytes.Buffer
	if err := RenderJSON(&buf, cols, rows); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	out := buf.String()
	// NDJSON: one JSON object per line
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %d", len(lines))
	}
	if !strings.Contains(lines[1], `"comment":null`) {
		t.Errorf("NULL not rendered as JSON null: %s", lines[1])
	}
	if !strings.Contains(lines[0], `"created_at":"2026-05-16T10:30:45Z"`) {
		t.Errorf("time not RFC3339 in: %s", lines[0])
	}
}

func TestRender_CSV_Escaping(t *testing.T) {
	cols := []string{"a", "b"}
	rows := [][]interface{}{
		{"hello, world", `quote "inside"`},
		{"line1\nline2", nil},
	}
	var buf bytes.Buffer
	if err := RenderCSV(&buf, cols, rows); err != nil {
		t.Fatalf("RenderCSV: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `"hello, world"`) {
		t.Errorf("comma not quoted: %s", out)
	}
	if !strings.Contains(out, `"quote ""inside"""`) {
		t.Errorf("inner quote not escaped: %s", out)
	}
}

func TestRender_JSON_NoHTMLEscape(t *testing.T) {
	cols := []string{"sql"}
	rows := [][]interface{}{{"a < 10 & b > 1"}}
	var buf bytes.Buffer
	if err := RenderJSON(&buf, cols, rows); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "a < 10 & b > 1") {
		t.Errorf("expected raw HTML chars, got %s", out)
	}
}

func TestIsReadStatement(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"SELECT * FROM t", true},
		{"select 1", true},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", true},
		{"PRAGMA table_info(x)", true},
		{"EXPLAIN SELECT 1", true},
		{"SHOW TABLES", true},
		{"DESC tbl", true},
		{"DESCRIBE tbl", true},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET a=1", false},
		{"DELETE FROM t", false},
		{"CREATE TABLE t (a int)", false},
		{"DROP TABLE t", false},
		{"BEGIN", false},
		{"COMMIT", false},
	}
	for _, c := range cases {
		if got := isReadStatement(c.in); got != c.want {
			t.Errorf("isReadStatement(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestRunSingleShot_SQLite(t *testing.T) {
	dir := t.TempDir()
	db, _, err := OpenDB("local", map[string]interface{}{
		"config_dir":  dir,
		"config_file": "testdb",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Exec("INSERT INTO t VALUES (1, 'alice')").Error; err != nil {
		t.Fatalf("insert: %v", err)
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()

	var buf bytes.Buffer
	opts := Options{
		ConfMap: map[string]interface{}{
			"config_dir":  dir,
			"config_file": "testdb",
		},
		Policy: "local",
		Query:  "SELECT * FROM t",
		Format: "json",
		Out:    &buf,
		ErrOut: &buf,
	}
	if err := RunSingleShot(opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `"name":"alice"`) {
		t.Errorf("expected alice in output, got:\n%s", out)
	}
}

func TestRunSingleShot_JSON_StdoutClean(t *testing.T) {
	dir := t.TempDir()
	db, _, err := OpenDB("local", map[string]interface{}{
		"config_dir":  dir,
		"config_file": "testdb",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Exec("INSERT INTO t VALUES (1, 'alice')").Error; err != nil {
		t.Fatalf("insert: %v", err)
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()

	var stdout, stderr bytes.Buffer
	opts := Options{
		ConfMap: map[string]interface{}{
			"config_dir":  dir,
			"config_file": "testdb",
		},
		Policy: "local",
		Query:  "SELECT * FROM t",
		Format: "json",
		Out:    &stdout,
		ErrOut: &stderr,
	}
	if err := RunSingleShot(opts); err != nil {
		t.Fatalf("run: %v", err)
	}

	// stdout must be parseable as NDJSON (no summary line)
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		if line == "" {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Errorf("stdout line not valid JSON: %q (full stdout=%q)", line, stdout.String())
		}
	}

	// The summary line should land on stderr
	if !strings.Contains(stderr.String(), "1 row in set") {
		t.Errorf("expected summary on stderr, got: %q", stderr.String())
	}
}

func TestRunSingleShot_TypeFidelity_JSON(t *testing.T) {
	dir := t.TempDir()
	db, _, err := OpenDB("local", map[string]interface{}{
		"config_dir":  dir,
		"config_file": "testdb",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Exec(`CREATE TABLE typed (
		i INTEGER, f REAL, s TEXT, b BLOB, n INTEGER
	)`).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Exec(`INSERT INTO typed VALUES (42, 3.14, 'hi', X'DEADBEEF', NULL)`).Error; err != nil {
		t.Fatalf("insert: %v", err)
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()

	var stdout, stderr bytes.Buffer
	opts := Options{
		ConfMap: map[string]interface{}{"config_dir": dir, "config_file": "testdb"},
		Policy:  "local",
		Query:   "SELECT i, f, s, n FROM typed",
		Format:  "json",
		Out:     &stdout,
		ErrOut:  &stderr,
	}
	if err := RunSingleShot(opts); err != nil {
		t.Fatalf("run: %v", err)
	}
	out := strings.TrimSpace(stdout.String())
	// Integer 42 must be unquoted; float 3.14 unquoted; string "hi" quoted; n null.
	if !strings.Contains(out, `"i":42`) {
		t.Errorf("integer not preserved as number: %q", out)
	}
	if !strings.Contains(out, `"f":3.14`) {
		t.Errorf("float not preserved as number: %q", out)
	}
	if !strings.Contains(out, `"s":"hi"`) {
		t.Errorf("string not preserved: %q", out)
	}
	if !strings.Contains(out, `"n":null`) {
		t.Errorf("NULL not preserved as null: %q", out)
	}
}

func TestRun_UnknownFormat(t *testing.T) {
	var stdout, stderr bytes.Buffer
	opts := Options{
		Policy:  "local",
		ConfMap: map[string]interface{}{},
		Query:   "SELECT 1",
		Format:  "yaml",
		Out:     &stdout,
		ErrOut:  &stderr,
	}
	err := Run(opts)
	if err == nil {
		t.Fatalf("expected error for unknown format")
	}
	if !strings.Contains(err.Error(), "unknown --format") {
		t.Errorf("unexpected error message: %v", err)
	}
}

type failingWriter struct {
	failAfter int
	count     int
}

func (fw *failingWriter) Write(p []byte) (int, error) {
	fw.count++
	if fw.count >= fw.failAfter {
		return 0, fmt.Errorf("simulated write failure")
	}
	return len(p), nil
}

func TestRender_WriteError(t *testing.T) {
	cols, rows := sampleRows()
	fw := &failingWriter{failAfter: 1}
	err := RenderJSON(fw, cols, rows)
	if err == nil {
		t.Errorf("expected RenderJSON to surface write error")
	}
}
