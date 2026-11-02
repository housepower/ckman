package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const startupCacheFile = "remote-config-last-good.json"

type startupCache struct {
	Format  string `json:"format"`
	Content []byte `json:"content"`
}

// LoadRemoteOrCache first asks the configured source, then falls back to the last
// configuration which CKMAN successfully parsed and decrypted.
func LoadRemoteOrCache(src ConfigSource, cacheDir string) (data []byte, format, origin string, err error) {
	data, format, remoteErr := src.Load()
	if remoteErr == nil && len(data) > 0 {
		return data, format, "remote", nil
	}
	cached, cacheErr := loadStartupCache(cacheDir)
	if cacheErr == nil {
		return cached.Content, cached.Format, "cache", nil
	}
	if remoteErr != nil {
		return nil, "", "", fmt.Errorf("remote load failed: %v; cached config unavailable: %w", remoteErr, cacheErr)
	}
	return nil, "", "", fmt.Errorf("remote config is empty; cached config unavailable: %w", cacheErr)
}

// LoadCachedConfig returns the last accepted remote configuration.
func LoadCachedConfig(cacheDir string) ([]byte, string, error) {
	cached, err := loadStartupCache(cacheDir)
	if err != nil {
		return nil, "", err
	}
	return cached.Content, cached.Format, nil
}

// SaveStartupCache persists only an already accepted configuration.
func SaveStartupCache(cacheDir, format string, data []byte) error {
	if len(data) == 0 {
		return errors.New("refuse to cache empty remote config")
	}
	b, err := json.Marshal(startupCache{Format: format, Content: data})
	if err != nil {
		return err
	}
	dir := filepath.Join(cacheDir, "config-cache")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".remote-config-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, startupCacheFile))
}

func loadStartupCache(cacheDir string) (startupCache, error) {
	b, err := os.ReadFile(filepath.Join(cacheDir, "config-cache", startupCacheFile))
	if err != nil {
		return startupCache{}, err
	}
	var cached startupCache
	if err := json.Unmarshal(b, &cached); err != nil {
		return startupCache{}, err
	}
	if cached.Format == "" || len(cached.Content) == 0 {
		return startupCache{}, errors.New("cached config is incomplete")
	}
	return cached, nil
}
