package sqlcli

import (
	"testing"

	"github.com/housepower/ckman/repository"
	driver "github.com/wanlay/gorm-dm8"
	_ "github.com/wanlay/gorm-dm8/dmr"
)

// gorm-dm8 的 Initialize 会用回调注册的返回值覆盖 sql.Open 的 err:DSN 解析失败时
// ConnPool 是 nil *sql.DB 却不报错,交给 gorm 自动 Ping 会 SIGSEGV(与 ckman 主程序
// 启动 panic 同根因)。repository.OpenGormDB 必须把这种情形变成普通 error。
func TestOpenGormDBBadDialectorReturnsError(t *testing.T) {
	// 不经过 BuildDialector,直接构造一个解析必失败的 DSN(# 截断出非法端口)
	dialector := driver.Open("dm://ckman:ckman#pwd123@192.168.1.10:5236?autoCommit=true")
	db, err := repository.OpenGormDB(dialector, nil)
	if err == nil {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		t.Fatalf("坏 DSN 应返回 error, 得到 db=%v", db)
	}
}

// dm8 后端连不上时应返回连接错误,而不是 panic 或卡死。
func TestOpenDB_DM8UnreachableReturnsErrorNotPanic(t *testing.T) {
	db, _, err := OpenDB("dm8", map[string]interface{}{
		"host":     "127.0.0.1",
		"port":     15239, // 无人监听的端口,connect 立即被拒
		"user":     "ckman",
		"password": "ckman#pwd123",
	})
	if err == nil {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		t.Fatal("连不上的端点应返回 error")
	}
}
