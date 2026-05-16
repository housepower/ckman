package dumpjson

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository/sqlite"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func TestDumpFromDB_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	sp := sqlite.NewSQLitePersistent()
	if err := sp.Init(sqlite.LocalConfig{ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := sp.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1", Comment: "x"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	dbPath := filepath.Join(dir, "clusters.db")
	outPath := filepath.Join(dir, "dump.json")
	if err := DumpFromDB(dbPath, outPath); err != nil {
		t.Fatalf("dump: %v", err)
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("parse dump: %v", err)
	}
	clusters, ok := data["clusters"].(map[string]interface{})
	if !ok || clusters["ck1"] == nil {
		t.Fatalf("dump missing ck1: %+v", data)
	}
}
