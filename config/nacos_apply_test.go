package config

import (
	"testing"
)

func TestParseRemote_Hjson(t *testing.T) {
	t.Cleanup(ResetForTest)
	data := []byte(`{
  server: {
    session_timeout: 7200
  }
  log: {
    level: "DEBUG"
  }
}`)
	got, err := parseRemote(data, ".hjson")
	if err != nil {
		t.Fatalf("parseRemote returned error: %v", err)
	}
	if got.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout=%d, want 7200", got.Server.SessionTimeout)
	}
	if got.Log.Level != "DEBUG" {
		t.Errorf("Log.Level=%q, want DEBUG", got.Log.Level)
	}
}

func TestParseRemote_Yaml(t *testing.T) {
	t.Cleanup(ResetForTest)
	data := []byte("server:\n  session_timeout: 7200\nlog:\n  level: DEBUG\n")
	got, err := parseRemote(data, ".yaml")
	if err != nil {
		t.Fatalf("parseRemote returned error: %v", err)
	}
	if got.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout=%d, want 7200", got.Server.SessionTimeout)
	}
	if got.Log.Level != "DEBUG" {
		t.Errorf("Log.Level=%q, want DEBUG", got.Log.Level)
	}
}

func TestParseRemote_Json(t *testing.T) {
	t.Cleanup(ResetForTest)
	data := []byte(`{"server":{"session_timeout":7200},"log":{"level":"DEBUG"}}`)
	got, err := parseRemote(data, ".json")
	if err != nil {
		t.Fatalf("parseRemote returned error: %v", err)
	}
	if got.Server.SessionTimeout != 7200 {
		t.Errorf("SessionTimeout=%d, want 7200", got.Server.SessionTimeout)
	}
}

func TestParseRemote_UnknownFormat(t *testing.T) {
	t.Cleanup(ResetForTest)
	_, err := parseRemote([]byte("server: {}"), ".xml")
	if err == nil {
		t.Fatal("expected error for unsupported format, got nil")
	}
}
