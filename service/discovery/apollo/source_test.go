package apollo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func TestSign(t *testing.T) {
	// 期望值由 openssl 独立计算：
	// printf '<ts>\n<pathWithQuery>' | openssl dgst -sha1 -hmac <secret> -binary | base64
	u, _ := url.Parse("http://apollo:8080/notifications/v2?appId=ckman&cluster=default&notifications=%5B%7B%22namespaceName%22%3A%22ckman.json%22%2C%22notificationId%22%3A-1%7D%5D")
	got := sign("df23df3f59884980844ff3dada30fa97", "1700000000000", u)
	if want := "5rU5keCspe3iZM+IrFPO7IGOpxY="; got != want {
		t.Errorf("sign = %s, want %s", got, want)
	}
}

// fakeApollo 模拟 Config Service 的 /configs 与 /notifications/v2。
type fakeApollo struct {
	mu             sync.Mutex
	content        string // 空表示 namespace 未发布
	releaseKey     int
	notificationId int64
	changed        chan struct{}
	secret         string
	authFailures   int
}

func newFakeApollo() *fakeApollo {
	return &fakeApollo{changed: make(chan struct{}, 1)}
}

func (f *fakeApollo) publish(content string) {
	f.mu.Lock()
	f.content = content
	f.releaseKey++
	f.notificationId++
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *fakeApollo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.secret != "" {
		want := "Apollo ckman:" + sign(f.secret, r.Header.Get("Timestamp"), r.URL)
		if r.Header.Get("Authorization") != want {
			f.mu.Lock()
			f.authFailures++
			f.mu.Unlock()
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	switch {
	case r.URL.Path == "/configs/ckman/default/ckman.json":
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.content == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		rk := strconv.Itoa(f.releaseKey)
		if r.URL.Query().Get("releaseKey") == rk {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"configurations": map[string]string{"content": f.content},
			"releaseKey":     rk,
		})
	case r.URL.Path == "/notifications/v2":
		var req []struct {
			NotificationId int64 `json:"notificationId"`
		}
		_ = json.Unmarshal([]byte(r.URL.Query().Get("notifications")), &req)
		f.mu.Lock()
		id := f.notificationId
		f.mu.Unlock()
		if len(req) == 1 && req[0].NotificationId == id {
			select {
			case <-f.changed:
				f.mu.Lock()
				id = f.notificationId
				f.mu.Unlock()
			case <-time.After(2 * time.Second):
				w.WriteHeader(http.StatusNotModified)
				return
			case <-r.Context().Done():
				return
			}
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"namespaceName": "ckman.json", "notificationId": id}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newSource(t *testing.T, meta, secret, cacheDir string) *ConfigSource {
	t.Helper()
	c := config.CKManApolloConfig{Meta: meta, AppId: "ckman", Cluster: "default", Namespace: "ckman.json", Secret: secret}
	s, err := NewConfigSource(&c, "10.0.0.1", cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestLoadAndBackup(t *testing.T) {
	f := newFakeApollo()
	f.secret = "s3cret"
	srv := httptest.NewServer(f)
	dir := t.TempDir()

	s := newSource(t, srv.URL, "s3cret", dir)
	data, format, err := s.Load()
	if err != nil || data != nil {
		t.Fatalf("未发布时应返回空内容: data=%q err=%v", data, err)
	}

	f.publish(`{ log: { level: "DEBUG" } }`)
	data, format, err = s.Load()
	if err != nil || string(data) != `{ log: { level: "DEBUG" } }` || format != config.FORMAT_JSON {
		t.Fatalf("Load = %q, %q, %v", data, format, err)
	}
	if f.authFailures != 0 {
		t.Errorf("签名校验失败 %d 次", f.authFailures)
	}

	// Apollo 不可达时用本地备份启动
	srv.Close()
	s2 := newSource(t, srv.URL, "s3cret", dir)
	data, _, err = s2.Load()
	if err != nil || string(data) != `{ log: { level: "DEBUG" } }` {
		t.Fatalf("备份回退失败: data=%q err=%v", data, err)
	}

	// 既不可达又没有备份时返回错误，由上层回退到本地配置
	s3 := newSource(t, srv.URL, "s3cret", t.TempDir())
	if _, _, err = s3.Load(); err == nil {
		t.Fatal("无备份且不可达时应返回错误")
	}
}

func TestMultipleMetas(t *testing.T) {
	f := newFakeApollo()
	f.publish(`{}`)
	srv := httptest.NewServer(f)
	defer srv.Close()

	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	s := newSource(t, dead.URL+", "+srv.URL+"/", "", t.TempDir())
	if data, _, err := s.Load(); err != nil || string(data) != `{}` {
		t.Fatalf("第一个 meta 不可达时应尝试下一个: data=%q err=%v", data, err)
	}
}

func TestWatch(t *testing.T) {
	f := newFakeApollo()
	f.publish(`{ cron: { enabled: true } }`)
	srv := httptest.NewServer(f)
	defer srv.Close()

	s := newSource(t, srv.URL, "", t.TempDir())
	if _, _, err := s.Load(); err != nil {
		t.Fatal(err)
	}

	got := make(chan string, 4)
	if err := s.Watch(func(data []byte, format string) { got <- string(data) }); err != nil {
		t.Fatal(err)
	}

	// 首次 notificationId=-1 会立即返回，但 releaseKey 未变，不应回调
	select {
	case d := <-got:
		t.Fatalf("内容未变却收到回调: %q", d)
	case <-time.After(500 * time.Millisecond):
	}

	f.publish(`{ cron: { enabled: false } }`)
	select {
	case d := <-got:
		if d != `{ cron: { enabled: false } }` {
			t.Fatalf("回调内容 = %q", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("发布后未收到回调")
	}
}

func TestNamespaceFormat(t *testing.T) {
	cases := map[string]string{
		"ckman.json": config.FORMAT_JSON,
		"ckman.yaml": config.FORMAT_YAML,
		"ckman.yml":  config.FORMAT_YAML,
		"ckman":      "",
		"ckman.txt":  "",
	}
	for ns, want := range cases {
		c := config.CKManApolloConfig{Meta: "http://x", AppId: "ckman", Cluster: "default", Namespace: ns}
		s, err := NewConfigSource(&c, "", t.TempDir())
		if want == "" {
			if err == nil {
				t.Errorf("namespace %s 应被拒绝", ns)
			}
			continue
		}
		if err != nil || s.format != want {
			t.Errorf("namespace %s: format=%v err=%v, want %s", ns, s, err, want)
		}
	}
}
