// Package apollo 基于 Apollo 的 HTTP 接口实现配置中心，不引入 agollo SDK（其依赖会抬高
// go.mod 的 Go 版本并带入 viper 等一串依赖）。用到的接口：
//
//	GET /configs/{appId}/{cluster}/{namespace}?releaseKey=&ip=   拉取配置
//	GET /notifications/v2?appId=&cluster=&notifications=        长轮询变更通知（服务端最多挂起 60s）
//
// namespace 必须是 json / yaml 类型，整份配置存放在 configurations.content 中。
package apollo

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
)

const (
	notifyTimeout  = 90 * time.Second // 必须大于服务端 60s 的挂起时间
	fetchTimeout   = 10 * time.Second
	initialBackoff = 1 * time.Second
	maxBackoff     = 60 * time.Second
)

var errNotFound = errors.New("apollo namespace not found or not released")

type ConfigSource struct {
	metas     []string
	appId     string
	cluster   string
	namespace string
	secret    string
	clientIp  string
	format    string
	backup    string
	client    *http.Client

	mu         sync.Mutex
	releaseKey string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewConfigSource clientIp 用于 Apollo 按 IP 灰度发布；cacheDir 下保存最近一次成功拉取的
// 配置，Apollo 不可达时用它启动。
func NewConfigSource(c *config.CKManApolloConfig, clientIp, cacheDir string) (*ConfigSource, error) {
	var metas []string
	for _, m := range strings.Split(c.Meta, ",") {
		if m = strings.TrimRight(strings.TrimSpace(m), "/"); m != "" {
			metas = append(metas, m)
		}
	}
	if len(metas) == 0 {
		return nil, errors.New("apollo.meta is required")
	}
	if c.AppId == "" || c.Cluster == "" {
		return nil, errors.New("apollo.app_id and apollo.cluster are required")
	}
	var format string
	switch strings.ToLower(filepath.Ext(c.Namespace)) {
	case ".json":
		format = config.FORMAT_JSON
	case ".yaml", ".yml":
		format = config.FORMAT_YAML
	default:
		return nil, fmt.Errorf("apollo.namespace %q must be a json or yaml namespace (e.g. ckman.json)", c.Namespace)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ConfigSource{
		metas:     metas,
		appId:     c.AppId,
		cluster:   c.Cluster,
		namespace: c.Namespace,
		secret:    c.Secret,
		clientIp:  clientIp,
		format:    format,
		backup:    filepath.Join(cacheDir, "apollo", fmt.Sprintf("%s+%s+%s", c.AppId, c.Cluster, c.Namespace)),
		client:    &http.Client{},
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (s *ConfigSource) Load() ([]byte, string, error) {
	content, err := s.fetch("")
	switch {
	case err == nil:
		s.saveBackup(content)
		return content, s.format, nil
	case errors.Is(err, errNotFound):
		return nil, s.format, nil
	}
	data, berr := os.ReadFile(s.backup)
	if berr != nil {
		return nil, "", err
	}
	log.Logger.Warnf("apollo unreachable, use local backup %s: %v", s.backup, err)
	return data, s.format, nil
}

func (s *ConfigSource) Watch(fn func([]byte, string)) error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		notificationId := int64(-1)
		backoff := initialBackoff
		for {
			id, changed, err := s.poll(notificationId)
			if err == nil && changed {
				s.mu.Lock()
				releaseKey := s.releaseKey
				s.mu.Unlock()
				var content []byte
				content, err = s.fetch(releaseKey)
				if err == nil {
					// 拉取成功后才前移通知编号：失败时下一轮仍带旧编号轮询，会立即再次收到这次变更。
					notificationId = id
					if content != nil {
						s.saveBackup(content)
						fn(content, s.format)
					}
				}
			}
			if err != nil {
				if s.ctx.Err() != nil {
					return
				}
				log.Logger.Warnf("apollo watch %s failed: %v", s.namespace, err)
				if !sleepCtx(s.ctx, backoff) {
					return
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			backoff = initialBackoff
			if s.ctx.Err() != nil {
				return
			}
		}
	}()
	return nil
}

func (s *ConfigSource) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}

// fetch 拉取配置。releaseKey 非空且服务端未发布新版本时返回 (nil, nil)。
func (s *ConfigSource) fetch(releaseKey string) ([]byte, error) {
	q := url.Values{}
	if releaseKey != "" {
		q.Set("releaseKey", releaseKey)
	}
	if s.clientIp != "" {
		q.Set("ip", s.clientIp)
	}
	path := fmt.Sprintf("/configs/%s/%s/%s",
		url.PathEscape(s.appId), url.PathEscape(s.cluster), url.PathEscape(s.namespace))
	status, body, err := s.get(path, q, fetchTimeout)
	if err != nil {
		return nil, err
	}
	switch status {
	case http.StatusOK:
	case http.StatusNotModified:
		return nil, nil
	case http.StatusNotFound:
		return nil, errNotFound
	default:
		return nil, fmt.Errorf("apollo get config: http %d: %s", status, truncate(body))
	}
	var resp struct {
		Configurations map[string]string `json:"configurations"`
		ReleaseKey     string            `json:"releaseKey"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("apollo decode config: %w", err)
	}
	content, ok := resp.Configurations["content"]
	if !ok {
		return nil, fmt.Errorf("apollo namespace %s has no content key, is it a properties namespace?", s.namespace)
	}
	s.mu.Lock()
	s.releaseKey = resp.ReleaseKey
	s.mu.Unlock()
	return []byte(content), nil
}

// poll 长轮询变更通知，返回新的 notificationId 以及是否有变更。
func (s *ConfigSource) poll(notificationId int64) (int64, bool, error) {
	notifications, _ := json.Marshal([]map[string]any{
		{"namespaceName": s.namespace, "notificationId": notificationId},
	})
	q := url.Values{}
	q.Set("appId", s.appId)
	q.Set("cluster", s.cluster)
	q.Set("notifications", string(notifications))
	status, body, err := s.get("/notifications/v2", q, notifyTimeout)
	if err != nil {
		return 0, false, err
	}
	switch status {
	case http.StatusNotModified:
		return notificationId, false, nil
	case http.StatusOK:
	default:
		return 0, false, fmt.Errorf("apollo notifications: http %d: %s", status, truncate(body))
	}
	var resp []struct {
		NamespaceName  string `json:"namespaceName"`
		NotificationId int64  `json:"notificationId"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, false, fmt.Errorf("apollo decode notifications: %w", err)
	}
	for _, n := range resp {
		if n.NamespaceName == s.namespace {
			return n.NotificationId, true, nil
		}
	}
	return notificationId, false, nil
}

// get 依次尝试每个 meta 地址，返回第一个非 5xx 的响应。连接失败或 5xx 时换下一个地址；
// 全部 5xx 时返回最后一个响应，由调用方按状态码报错。
func (s *ConfigSource) get(path string, q url.Values, timeout time.Duration) (int, []byte, error) {
	rawQuery := q.Encode()
	var (
		lastErr    error
		lastStatus int
		lastBody   []byte
	)
	for _, meta := range s.metas {
		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		status, body, err := s.do(ctx, meta+path, rawQuery)
		cancel()
		if err == nil && status < http.StatusInternalServerError {
			return status, body, nil
		}
		if err == nil {
			lastStatus, lastBody = status, body
		} else {
			lastErr = err
		}
		if s.ctx.Err() != nil {
			break
		}
	}
	if lastStatus != 0 {
		return lastStatus, lastBody, nil
	}
	return 0, nil, lastErr
}

func (s *ConfigSource) do(ctx context.Context, rawURL, rawQuery string) (int, []byte, error) {
	if rawQuery != "" {
		rawURL += "?" + rawQuery
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, nil, err
	}
	if s.secret != "" {
		ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
		req.Header.Set("Authorization", fmt.Sprintf("Apollo %s:%s", s.appId, sign(s.secret, ts, req.URL)))
		req.Header.Set("Timestamp", ts)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

// sign 按 Apollo 访问密钥规则计算签名：Base64(HmacSHA1(secret, timestamp + "\n" + pathWithQuery))。
func sign(secret, timestamp string, u *url.URL) string {
	pathWithQuery := u.EscapedPath()
	if u.RawQuery != "" {
		pathWithQuery += "?" + u.RawQuery
	}
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(timestamp + "\n" + pathWithQuery))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (s *ConfigSource) saveBackup(content []byte) {
	if err := os.MkdirAll(filepath.Dir(s.backup), 0o750); err != nil {
		log.Logger.Warnf("apollo create backup dir failed: %v", err)
		return
	}
	tmp := s.backup + ".tmp"
	if err := os.WriteFile(tmp, content, 0o600); err != nil {
		log.Logger.Warnf("apollo write backup failed: %v", err)
		return
	}
	if err := os.Rename(tmp, s.backup); err != nil {
		log.Logger.Warnf("apollo rename backup failed: %v", err)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func truncate(b []byte) string {
	const n = 200
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
