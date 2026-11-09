package log

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInitLoggerDefaultFallsBackWhenLogPathCannotBeOpened(t *testing.T) {
	blockedParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedParent, []byte("file"), 0o600); err != nil {
		t.Fatalf("create blocking file: %v", err)
	}

	err := InitLoggerDefault("debug", []string{filepath.Join(blockedParent, "ckmanctl.log")})
	if err == nil {
		t.Fatal("expected an error for an unusable log path")
	}
	if Logger == nil || ZapLog == nil {
		t.Fatal("expected stderr fallback logger to be initialized")
	}

	Logger.Info("stderr fallback remains usable")
}
