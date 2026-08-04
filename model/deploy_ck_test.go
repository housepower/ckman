package model

import (
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestPackTlsConf(t *testing.T) {
	origin := &TlsConf{Cert: "CERT-PEM", Key: "SECRET-KEY", SelfSigned: true}
	conf := CKManClickHouseConfig{
		Password: "123456",
		Secure:   true,
		TlsConf:  origin,
	}
	conf.Pack()

	// 私钥脱敏，证书原样返回（证书本就是公开信息）
	assert.Equal(t, "**********", conf.TlsConf.Key)
	assert.Equal(t, "CERT-PEM", conf.TlsConf.Cert)
	assert.True(t, conf.TlsConf.SelfSigned)

	// 关键：不得原地修改。local repository 下该指针与内存中的集群配置共享，
	// 原地改会把真实私钥抹成星号
	assert.Equal(t, "SECRET-KEY", origin.Key)
}

func TestPackTlsConfNil(t *testing.T) {
	conf := CKManClickHouseConfig{Password: "123456"}
	conf.Pack()
	assert.Nil(t, conf.TlsConf)
}

func TestUnPackTlsConf(t *testing.T) {
	stored := CKManClickHouseConfig{
		Password: "123456",
		TlsConf:  &TlsConf{Cert: "OLD-CERT", Key: "REAL-KEY", SelfSigned: true},
	}
	// 前端把脱敏后的星号原样提交回来
	incoming := CKManClickHouseConfig{
		Password: "******",
		TlsConf:  &TlsConf{Cert: "OLD-CERT", Key: "**********"},
	}
	incoming.UnPack(stored)
	assert.Equal(t, "REAL-KEY", incoming.TlsConf.Key)
}

func TestUnPackTlsConfNewKey(t *testing.T) {
	stored := CKManClickHouseConfig{
		TlsConf: &TlsConf{Cert: "OLD-CERT", Key: "REAL-KEY"},
	}
	// 用户传了新私钥，必须保留新值
	incoming := CKManClickHouseConfig{
		TlsConf: &TlsConf{Cert: "NEW-CERT", Key: "NEW-KEY"},
	}
	incoming.UnPack(stored)
	assert.Equal(t, "NEW-KEY", incoming.TlsConf.Key)
}

func TestUnPackTlsConfFromNil(t *testing.T) {
	// 明文集群首次切 TLS：库里没有 TlsConf，不能 panic
	stored := CKManClickHouseConfig{}
	incoming := CKManClickHouseConfig{
		TlsConf: &TlsConf{Cert: "NEW-CERT", Key: "NEW-KEY"},
	}
	incoming.UnPack(stored)
	assert.Equal(t, "NEW-KEY", incoming.TlsConf.Key)
}

// TestNormalizeSecurePorts_SecureEnabled_SwitchesDefaultPorts is the
// regression test for the port/protocol confusion bug: a real cluster was
// switched Secure=false -> true while HttpPort stayed pinned at the
// plaintext default 8123 (because the config page locked the field), so
// custom.xml ended up serving HTTPS on the port everyone assumes is
// plaintext. NormalizeSecurePorts is the backend-side safety net for this -
// it must fire regardless of whether the frontend's Secure-linked port
// switcher ran at all (direct API calls, scripted deploys, ...).
func TestNormalizeSecurePorts_SecureEnabled_SwitchesDefaultPorts(t *testing.T) {
	conf := CKManClickHouseConfig{
		Secure:   true,
		Port:     ClickHouseDefaultPort,
		HttpPort: ClickHouseDefaultHttpPort,
	}
	conf.NormalizeSecurePorts()
	assert.Equal(t, ClickHouseDefaultSecurePort, conf.Port)
	assert.Equal(t, ClickHouseDefaultSecureHttpPort, conf.HttpPort)
}

// TestNormalizeSecurePorts_SecureDisabled_SwitchesBackToPlaintextPorts is the
// mirror case: turning Secure off while the ports are still sitting at the
// TLS defaults must switch them back, so a plaintext cluster doesn't end up
// trying to speak plaintext on 9440/8443.
func TestNormalizeSecurePorts_SecureDisabled_SwitchesBackToPlaintextPorts(t *testing.T) {
	conf := CKManClickHouseConfig{
		Secure:   false,
		Port:     ClickHouseDefaultSecurePort,
		HttpPort: ClickHouseDefaultSecureHttpPort,
	}
	conf.NormalizeSecurePorts()
	assert.Equal(t, ClickHouseDefaultPort, conf.Port)
	assert.Equal(t, ClickHouseDefaultHttpPort, conf.HttpPort)
}

// TestNormalizeSecurePorts_CustomPorts_AreNeverOverwritten guards the "don't
// clobber a deliberate customization" half of the contract: a port that
// isn't sitting at either mode's well-known default (e.g. a user who put
// ClickHouse behind 9001) must survive the Secure flip untouched, in either
// direction.
func TestNormalizeSecurePorts_CustomPorts_AreNeverOverwritten(t *testing.T) {
	t.Run("secure enabled", func(t *testing.T) {
		conf := CKManClickHouseConfig{Secure: true, Port: 9001, HttpPort: 8001}
		conf.NormalizeSecurePorts()
		assert.Equal(t, 9001, conf.Port)
		assert.Equal(t, 8001, conf.HttpPort)
	})
	t.Run("secure disabled", func(t *testing.T) {
		conf := CKManClickHouseConfig{Secure: false, Port: 9001, HttpPort: 8001}
		conf.NormalizeSecurePorts()
		assert.Equal(t, 9001, conf.Port)
		assert.Equal(t, 8001, conf.HttpPort)
	})
}

// TestSingleNodeConfig_SecureClusterCarriesSecureAndProtocol is the direct
// regression test for the production incident this method fixes.
// GetCkClusterStatus (service/clickhouse/clickhouse.go) derives a per-node
// config to probe each node individually. Before the fix it hand-built that
// literal and copied Port/HttpPort/Cluster/User/Password but silently
// dropped Protocol and Secure, so on a cluster switched to TLS the derived
// config's Secure defaulted to false / Protocol to "" and GetConnOption()
// dialed the TLS port (9440) with the plaintext Native protocol - every
// probe failed and every node was marked CkStatusRed ("offline"), even
// though the same cluster served real queries fine over the (correctly
// built) full config. If this regresses, TLS clusters go back to showing
// every node offline in the UI.
func TestSingleNodeConfig_SecureClusterCarriesSecureAndProtocol(t *testing.T) {
	conf := CKManClickHouseConfig{
		Hosts:    []string{"192.168.1.1", "192.168.1.2"},
		Cluster:  "tls_cluster",
		Port:     ClickHouseDefaultSecurePort,
		HttpPort: ClickHouseDefaultSecureHttpPort,
		Protocol: clickhouse.Native.String(),
		Secure:   true,
		User:     "default",
		Password: "secret",
	}

	node := conf.SingleNodeConfig("192.168.1.1")

	assert.True(t, node.Secure)
	assert.Equal(t, conf.Protocol, node.Protocol)
	assert.Equal(t, conf.Port, node.Port)
	assert.Equal(t, conf.HttpPort, node.HttpPort)
	assert.Equal(t, []string{"192.168.1.1"}, node.Hosts)
	assert.Equal(t, conf.Cluster, node.Cluster)
}

// TestSingleNodeConfig_MatchesSourceConnOption is the anti-regression test
// for "someone adds a new connection-relevant field to CKManClickHouseConfig
// (or GetConnOption starts reading one it didn't before) and forgets to
// carry it in SingleNodeConfig" - exactly the class of bug that caused the
// production incident documented on SingleNodeConfig's doc comment (Protocol
// and Secure were silently dropped).
//
// Rather than hard-coding "these are the fields that matter" (which is
// exactly the assumption that bit us - Protocol/Secure weren't on anyone's
// list), this test fills *every* scalar (string/int/bool) field of a source
// config with a distinct non-zero sentinel value via reflection, derives a
// single-node config from it, and asserts GetConnOption() - the actual
// function that decides how we dial a node - produces byte-identical output
// for both. GetConnOption() only reaches the fields it currently reads, so
// this comparison is only as strong as GetConnOption()'s own field list -
// but that's precisely the right dependency: the moment GetConnOption is
// changed to read one more field (e.g. today's hard-coded
// InsecureSkipVerify becomes driven by a new config field), the source
// config will carry a non-zero sentinel for it, SingleNodeConfig won't copy
// it (until someone fixes that), and the two GetConnOption() results will
// diverge - failing this test instead of shipping a silent "looks connected
// in code review, fails on a live TLS cluster" bug.
//
// It runs twice with different Protocol sentinels (HTTP and a non-HTTP
// value) so both branches of GetConnOption()'s Protocol switch - and
// therefore both HttpPort and Port - are exercised.
func TestSingleNodeConfig_MatchesSourceConnOption(t *testing.T) {
	fill := func(protocol string) CKManClickHouseConfig {
		var source CKManClickHouseConfig
		v := reflect.ValueOf(&source).Elem()
		typ := v.Type()
		for i := 0; i < typ.NumField(); i++ {
			f := v.Field(i)
			if !f.CanSet() {
				continue
			}
			switch f.Kind() {
			case reflect.String:
				f.SetString("sentinel-" + typ.Field(i).Name)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				f.SetInt(424242)
			case reflect.Bool:
				f.SetBool(true)
			}
		}
		source.Protocol = protocol
		return source
	}

	for _, protocol := range []string{clickhouse.HTTP.String(), clickhouse.Native.String()} {
		t.Run("protocol="+protocol, func(t *testing.T) {
			source := fill(protocol)
			derived := source.SingleNodeConfig("10.0.0.1")

			assert.Equal(t, source.GetConnOption(), derived.GetConnOption(),
				"SingleNodeConfig must carry every field GetConnOption() reads; "+
					"a mismatch here means a connection-relevant field was dropped")
		})
	}
}

// TestSingleNodeConfig_FieldClassification is a forcing function, not a
// behavioral test: every field of CKManClickHouseConfig must be explicitly
// sorted into "carried" (SingleNodeConfig copies it) or "excluded"
// (deliberately not copied, e.g. deploy/ssh/storage settings that a
// single-node connection probe has no business touching). Adding a field to
// the struct without touching either list fails this test, forcing whoever
// adds it to consciously decide - instead of the field quietly being absent
// from SingleNodeConfig the way Protocol/Secure were before this incident
// was fixed. TestSingleNodeConfig_MatchesSourceConnOption then verifies the
// "carried" claim is actually true for every field GetConnOption() reads.
func TestSingleNodeConfig_FieldClassification(t *testing.T) {
	carried := map[string]bool{
		"Hosts":    true, // rewritten to the single target host, not copied verbatim
		"Cluster":  true,
		"Port":     true,
		"HttpPort": true,
		"Protocol": true,
		"Secure":   true,
		"User":     true,
		"Password": true,
	}
	excluded := map[string]bool{
		"Comment":          true,
		"PkgType":          true,
		"PkgName":          true,
		"Version":          true,
		"Cwd":              true,
		"LogicCluster":     true,
		"Shards":           true,
		"TlsConf":          true, // see SingleNodeConfig doc comment: not needed to connect today
		"IsReplica":        true,
		"Keeper":           true,
		"KeeperConf":       true,
		"ZkNodes":          true,
		"ZkPort":           true,
		"PromHost":         true,
		"PromPort":         true,
		"PromMetricPort":   true,
		"EncryptType":      true,
		"Path":             true,
		"SshUser":          true,
		"AuthenticateType": true,
		"SshPassword":      true,
		"SshPort":          true,
		"Storage":          true,
		"UsersConf":        true,
		"Expert":           true,
		"NodeOverrides":    true,
		"Mode":             true,
		"NeedSudo":         true,
	}

	typ := reflect.TypeOf(CKManClickHouseConfig{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		inCarried, inExcluded := carried[name], excluded[name]
		if inCarried == inExcluded { // false==false (unclassified) or true==true (both, a mistake)
			t.Errorf("field %q of CKManClickHouseConfig is not classified by this test: "+
				"add it to `carried` in SingleNodeConfig (and this test) if it affects connection "+
				"behavior or is context callers need, or to `excluded` with a reason if it "+
				"deliberately should not follow a single-node config derivation", name)
		}
	}
}
