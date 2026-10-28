package dm8

import (
	"database/sql"
	"net/url"
	"strings"
	"testing"

	"github.com/housepower/ckman/log"
	driver "github.com/wanlay/gorm-dm8"
	_ "github.com/wanlay/gorm-dm8/dmr"
	"go.uber.org/zap"
)

func init() {
	log.ZapLog = zap.NewNop()
	log.Logger = log.ZapLog.Sugar()
}

// 密码里的 URL 特殊字符必须转义后再拼 DSN:达梦驱动用 net/url 解析 dm:// URL,
// 密码带 # 会把 DSN 截断(url.Parse 报 invalid port),sql.Open 返回 nil *sql.DB,
// 叠加 gorm-dm8 Initialize 吞 err 的问题,最终在 gorm 自动 Ping 里 SIGSEGV。
// 这是线上 ckman 起不来的根因(密码含 #)。
func TestBuildDialectorEscapesSpecialChars(t *testing.T) {
	cases := []struct{ user, password string }{
		{"ckman", "ckman#pwd123"},
		{"ckman", "p@ss?w%rd/&: x"},
		{"user#1", "plain"},
	}
	for _, tc := range cases {
		dialector, err := BuildDialector(map[string]interface{}{
			"host":     "192.168.1.10",
			"port":     5236,
			"user":     tc.user,
			"password": tc.password,
		})
		if err != nil {
			t.Fatalf("user=%q: BuildDialector: %v", tc.user, err)
		}
		dsn := dialector.(*driver.Dialector).DSN

		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("user=%q password=%q: dsn %q 无法被 url.Parse 解析: %v", tc.user, tc.password, dsn, err)
		}
		if got := u.User.Username(); got != tc.user {
			t.Fatalf("dsn %q: user 应为 %q, 得到 %q", dsn, tc.user, got)
		}
		pwd, _ := u.User.Password()
		if pwd != tc.password {
			t.Fatalf("dsn %q: password 应为 %q, 得到 %q", dsn, tc.password, pwd)
		}
		if u.Host != "192.168.1.10:5236" {
			t.Fatalf("dsn %q: host 应为 192.168.1.10:5236, 得到 %q", dsn, u.Host)
		}
		if u.Query().Get("autoCommit") != "true" {
			t.Fatalf("dsn %q: autoCommit=true 丢失", dsn)
		}

		// 驱动自己也必须能解析(sql.Open 只在解析失败时返回 error)
		db, err := sql.Open("dm", dsn)
		if err != nil || db == nil {
			t.Fatalf("dsn %q: 驱动解析失败: db=%v err=%v", dsn, db, err)
		}
		_ = db.Close()
	}
}

// gorm-dm8 的 Initialize 会用回调注册的返回值覆盖 sql.Open 的 err:DSN 解析失败时
// ConnPool 是 nil *sql.DB 却不报错,直接交给 gorm 自动 Ping 会 SIGSEGV。
// openGormDB 必须把这种情形变成普通 error。
func TestOpenGormDBBadDSNReturnsErrorNotPanic(t *testing.T) {
	// 不经过 BuildDialector,直接构造一个解析必失败的 DSN(# 截断出非法端口)
	dialector := driver.Open("dm://ckman:ckman#pwd123@192.168.1.10:5236?autoCommit=true")
	db, err := openGormDB(dialector)
	if err == nil {
		t.Fatalf("坏 DSN 应返回 error, 得到 db=%v", db)
	}
}

// 端到端:特殊字符密码 + 连不上的端点,Init 应返回连接错误而不是 panic。
func TestInitSpecialCharPasswordNoPanic(t *testing.T) {
	p := NewDM8Persistent()
	err := p.Init(DM8Config{
		Host:     "127.0.0.1",
		Port:     15239, // 无人监听的端口,connect 立即被拒
		User:     "ckman",
		Password: "ckman#pwd123",
	})
	if err == nil {
		t.Fatal("连不上的端点应返回 error")
	}
	// 错误应当是连接层错误,而不是 DSN 解析被截断后的奇怪报错
	if strings.Contains(err.Error(), "invalid port") {
		t.Fatalf("密码未被转义, DSN 被 # 截断: %v", err)
	}
}
