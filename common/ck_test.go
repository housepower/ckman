package common

import (
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestCompareClickHouseVersion(t *testing.T) {
	assert.Equal(t, 1, CompareClickHouseVersion("21.9.3.45", "20.10.8.2403"))
	assert.Equal(t, 1, CompareClickHouseVersion("21.9.3.45", "21.7"))
	assert.Equal(t, 1, CompareClickHouseVersion("21.10.3.45", "21.9.8.2403"))
	assert.Equal(t, 1, CompareClickHouseVersion("21.9.3.1018", "20.9.3.45"))
	assert.Equal(t, 0, CompareClickHouseVersion("21.9.3.45", "21.9.x.x"))
	assert.Equal(t, 0, CompareClickHouseVersion("21.9.3.45", "21.9"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.x.x", "21.10.x.x"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.3.45", "21.9.3.2403"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.3", "21.10.8.2403"))
	assert.Equal(t, 0, CompareClickHouseVersion("22.3.3.44", "22.x"))
	assert.Equal(t, -1, CompareClickHouseVersion("21.9.3.45", "22.x"))
}

func TestCkPasswd(t *testing.T) {
	passwd := "cyc2010"
	assert.Equal(t, "a40943925ca51a95de7d39bc8c31757207d53b5e7114e695c04db63b6868f3e1", CkPassword(passwd, SHA256_HEX))
	assert.Equal(t, "812b8fad11eb25a3cf4cc2c54ae10a4948a0c25b", CkPassword(passwd, DOUBLE_SHA1_HEX))
	assert.Equal(t, passwd, CkPassword(passwd, PLAINTEXT))

	assert.Equal(t, "a5e5af71ae6063f531f6d5e0982e7e3741f09e3876b24d0cb89f2820c34fc065", CkPassword("4ADUUA5O", SHA256_HEX))
	assert.Equal(t, "15fae939139a48b92e715609560e487298568076", CkPassword("FKP8OfUF", DOUBLE_SHA1_HEX))

}

func TestCkPasswdLabel(t *testing.T) {
	assert.Equal(t, "password_sha256_hex", CkPasswdLabel(SHA256_HEX))
	assert.Equal(t, "password_double_sha1_hex", CkPasswdLabel(DOUBLE_SHA1_HEX))
	assert.Equal(t, "password", CkPasswdLabel(PLAINTEXT))
	assert.Equal(t, "password", CkPasswdLabel(-1))
}

func TestPickAvailableSchemaSource(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Hosts: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"},
	}
	allOk := func(host string, _ model.ConnetOption) error { return nil }

	t.Run("全部可用返回首个", func(t *testing.T) {
		got := pickAvailableSchemaSource(conf, allOk)
		assert.Equal(t, "10.0.0.1", got)
	})

	t.Run("首个不可用跳到下一个", func(t *testing.T) {
		fn := func(host string, _ model.ConnetOption) error {
			if host == "10.0.0.1" {
				return errors.New("connection refused")
			}
			return nil
		}
		got := pickAvailableSchemaSource(conf, fn)
		assert.Equal(t, "10.0.0.2", got)
	})

	t.Run("全不可用返回空", func(t *testing.T) {
		fn := func(host string, _ model.ConnetOption) error { return errors.New("dead") }
		got := pickAvailableSchemaSource(conf, fn)
		assert.Equal(t, "", got)
	})

	t.Run("exclude 跳过指定 IP", func(t *testing.T) {
		got := pickAvailableSchemaSource(conf, allOk, "10.0.0.1")
		assert.Equal(t, "10.0.0.2", got)
	})

	t.Run("exclude 含空字符串不影响", func(t *testing.T) {
		got := pickAvailableSchemaSource(conf, allOk, "")
		assert.Equal(t, "10.0.0.1", got)
	})
}

// ---------------------------------------------------------------------
// connOrderId / connection-pool cache-key regression suite.
//
// Root cause: orderId (the connection-pool cache key) used to hash only
// Addr and Auth. A cluster with Protocol=http switching Secure off->on
// keeps the exact same host:HttpPort and credentials, so Addr/Auth alone
// never change - the pool would keep serving the pre-switch plaintext
// *Conn (whose connector has no TLS config at all) straight through a
// TLS cutover.
// ---------------------------------------------------------------------

func sameAddrAuth() (addr []string, auth clickhouse.Auth) {
	return []string{"10.0.0.1:8123"}, clickhouse.Auth{Database: "default", Username: "default", Password: "pw"}
}

// TestConnOrderId_ProtocolDiffers_ChangesOrderId is the regression test for
// the core bug: same Addr, same Auth, only Protocol differs (native vs
// http) - the resulting orderId must differ, otherwise a pool lookup can't
// tell the two apart.
func TestConnOrderId_ProtocolDiffers_ChangesOrderId(t *testing.T) {
	addr, auth := sameAddrAuth()
	nativeId, err := connOrderId(ConnBase{Addr: addr, Auth: auth, Protocol: clickhouse.Native, TLS: false})
	if err != nil {
		t.Fatalf("connOrderId (native): %v", err)
	}
	httpId, err := connOrderId(ConnBase{Addr: addr, Auth: auth, Protocol: clickhouse.HTTP, TLS: false})
	if err != nil {
		t.Fatalf("connOrderId (http): %v", err)
	}
	if nativeId == httpId {
		t.Errorf("expected different orderId for Protocol=native vs Protocol=http with identical Addr/Auth, got the same: %q", nativeId)
	}
}

// TestConnOrderId_TLSDiffers_ChangesOrderId is the exact incident scenario:
// same host:HttpPort, same credentials, only whether TLS is enabled differs
// (the plaintext-to-TLS cutover on a Protocol=http cluster). Without TLS in
// the hash, orderId is identical across the cutover and the pool serves the
// stale plaintext connection.
func TestConnOrderId_TLSDiffers_ChangesOrderId(t *testing.T) {
	addr, auth := sameAddrAuth()
	plainId, err := connOrderId(ConnBase{Addr: addr, Auth: auth, Protocol: clickhouse.HTTP, TLS: false})
	if err != nil {
		t.Fatalf("connOrderId (plaintext): %v", err)
	}
	tlsId, err := connOrderId(ConnBase{Addr: addr, Auth: auth, Protocol: clickhouse.HTTP, TLS: true})
	if err != nil {
		t.Fatalf("connOrderId (tls): %v", err)
	}
	if plainId == tlsId {
		t.Errorf("expected different orderId for TLS=false vs TLS=true with identical Addr/Auth/Protocol, got the same: %q", plainId)
	}
}

// TestConnOrderId_IdenticalInputs_SameOrderId is the sanity counterpart:
// unrelated calls with genuinely identical inputs must still hit the pool
// (same orderId), otherwise every connection would be needlessly rebuilt.
func TestConnOrderId_IdenticalInputs_SameOrderId(t *testing.T) {
	addr, auth := sameAddrAuth()
	id1, err := connOrderId(ConnBase{Addr: addr, Auth: auth, Protocol: clickhouse.Native, TLS: true})
	if err != nil {
		t.Fatalf("connOrderId (1): %v", err)
	}
	id2, err := connOrderId(ConnBase{Addr: addr, Auth: auth, Protocol: clickhouse.Native, TLS: true})
	if err != nil {
		t.Fatalf("connOrderId (2): %v", err)
	}
	if id1 != id2 {
		t.Errorf("expected identical orderId for identical inputs, got %q vs %q", id1, id2)
	}
}
