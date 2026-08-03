package controller

import (
	"testing"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/repository/sqlite"
)

// newTestPersistent spins up a throwaway SQLite-backed repository.Ps so that
// mergeClickhouseConfig / checkConfigParams can be exercised end to end,
// including the real EncodePasswd/DecodePasswd round trip that strips
// TlsConf.NotAfter on every read from the store.
func newTestPersistent(t *testing.T) *sqlite.SQLitePersistent {
	t.Helper()
	if log.Logger == nil {
		log.InitLoggerConsole()
	}
	sp := sqlite.NewSQLitePersistent()
	if err := sp.Init(sqlite.LocalConfig{ConfigDir: t.TempDir(), ConfigFile: "testdb"}); err != nil {
		t.Fatalf("init sqlite persistent: %v", err)
	}
	return sp
}

func baseTlsCluster(t *testing.T, name, certPEM, keyPEM string) model.CKManClickHouseConfig {
	t.Helper()
	return model.CKManClickHouseConfig{
		Cluster: name,
		Comment: "original comment",
		Port:    9000,
		Secure:  true,
		TlsConf: &model.TlsConf{
			Cert:       certPEM,
			Key:        keyPEM,
			SelfSigned: true,
		},
		SshUser: "root",
	}
}

// TestMergeClickhouseConfig_UnrelatedFieldChange_DoesNotTriggerRestart is the
// regression test for the NotAfter trap: model.TlsConf.NotAfter participates
// in JSON (despite swaggerignore only hiding it from swagger docs), so the
// frontend receives it from GetConfig and echoes it straight back on save.
// The copy loaded fresh from the repository never carries NotAfter (see
// repository.EncodePasswd/DecodePasswd, which always rebuild TlsConf without
// it). A naive reflect.DeepEqual(cluster.TlsConf, conf.TlsConf) would then
// see the two NotAfter values differ and report secureChanged==true on every
// single save, forcing a full cluster restart even when only an unrelated
// field (e.g. Comment) changed and the certificate itself is untouched.
func TestMergeClickhouseConfig_UnrelatedFieldChange_DoesNotTriggerRestart(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	certPEM, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	initial := baseTlsCluster(t, "tlstest", certPEM, keyPEM)
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Simulate what the frontend actually does: call GetConfig, which fills
	// NotAfter on a Pack()'d copy, then submit that payload straight back
	// with only the Comment field edited.
	stored, err := sp.GetClusterbyName("tlstest")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	display := stored
	display.Pack()
	if display.TlsConf == nil {
		t.Fatalf("expected TlsConf to survive Pack()")
	}
	notAfter, err := common.CertNotAfter(display.TlsConf.Cert)
	if err != nil {
		t.Fatalf("CertNotAfter: %v", err)
	}
	display.TlsConf.NotAfter = notAfter // this is what GetConfig sends to the browser

	submitted := display
	submitted.Comment = "updated comment only"
	// Key is masked with asterisks by Pack(); mergeClickhouseConfig's call to
	// conf.UnPack(cluster) is responsible for restoring the real value before
	// comparison, exactly like a real save request from the browser.

	restart, cc, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf("mergeClickhouseConfig returned error: %v", lerr)
	}
	if restart {
		t.Errorf("restart=true: an unrelated Comment-only change with an unchanged certificate must not trigger a cluster restart")
	}
	if cc {
		t.Errorf("cc=true: an unrelated Comment-only change with an unchanged certificate must not trigger a config re-push")
	}
}

// TestMergeClickhouseConfig_CertificateChange_TriggersRestart is the
// counterpart positive test: make sure the fix above didn't accidentally
// blind secureChanged to real certificate rotations.
func TestMergeClickhouseConfig_CertificateChange_TriggersRestart(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	certPEM, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}
	newCertPEM, newKeyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate second self-signed cert: %v", err)
	}

	initial := baseTlsCluster(t, "tlstest2", certPEM, keyPEM)
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stored, err := sp.GetClusterbyName("tlstest2")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	submitted := stored
	submitted.TlsConf = &model.TlsConf{
		Cert:       newCertPEM,
		Key:        newKeyPEM,
		SelfSigned: true,
	}

	restart, cc, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf("mergeClickhouseConfig returned error: %v", lerr)
	}
	if !restart {
		t.Errorf("restart=false: rotating the certificate must trigger a cluster restart")
	}
	if !cc {
		t.Errorf("cc=false: rotating the certificate must trigger a config re-push")
	}
}

func TestTlsConfEqual(t *testing.T) {
	certPEM, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	a := &model.TlsConf{Cert: certPEM, Key: keyPEM, SelfSigned: true, NotAfter: "2020-01-01 00:00:00"}
	b := &model.TlsConf{Cert: certPEM, Key: keyPEM, SelfSigned: true, NotAfter: ""}
	if !tlsConfEqual(a, b) {
		t.Errorf("tlsConfEqual should ignore NotAfter and treat these as equal")
	}

	c := &model.TlsConf{Cert: certPEM, Key: "different-key", SelfSigned: true}
	if tlsConfEqual(a, c) {
		t.Errorf("tlsConfEqual should report inequality when Key differs")
	}

	if tlsConfEqual(a, nil) || tlsConfEqual(nil, a) {
		t.Errorf("tlsConfEqual should report inequality when only one side is nil")
	}
	if !tlsConfEqual(nil, nil) {
		t.Errorf("tlsConfEqual(nil, nil) should be true")
	}
}

// TestCheckConfigParams_SecureRequiresValidCert exercises Step 1: enabling
// Secure without a certificate, or with an invalid one, must be rejected;
// a genuinely valid cert/key pair must pass.
func TestCheckConfigParams_SecureRequiresValidCert(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	certPEM, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	base := baseTlsCluster(t, "tlscheck", certPEM, keyPEM)
	if err := sp.CreateCluster(base); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	t.Run("missing TlsConf", func(t *testing.T) {
		conf := base
		conf.TlsConf = nil
		if err := checkConfigParams(&conf); err == nil {
			t.Errorf("expected error when Secure=true and TlsConf is nil")
		}
	})

	t.Run("mismatched cert/key", func(t *testing.T) {
		otherCertPEM, _, err := common.GenSelfSignedCert(nil)
		if err != nil {
			t.Fatalf("generate mismatched cert: %v", err)
		}
		conf := base
		conf.TlsConf = &model.TlsConf{Cert: otherCertPEM, Key: keyPEM}
		if err := checkConfigParams(&conf); err == nil {
			t.Errorf("expected error when cert and key don't match")
		}
	})

	t.Run("valid cert/key passes", func(t *testing.T) {
		conf := base
		conf.TlsConf = &model.TlsConf{Cert: certPEM, Key: keyPEM}
		if err := checkConfigParams(&conf); err != nil {
			t.Errorf("expected no error for a valid cert/key pair, got: %v", err)
		}
	})
}
