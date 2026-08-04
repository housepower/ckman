package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
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

// newGenSelfSignedCertContext builds a gin.Context wired to a JSON POST body,
// mirroring what the real router hands GenSelfSignedCert. The recorder is
// unused by the handler itself (results are captured via a fake wrapfunc
// below) but gin.CreateTestContext requires one.
func newGenSelfSignedCertContext(t *testing.T, body string) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/ck/cert/selfsigned", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return c
}

// TestGenSelfSignedCert_EmptyHosts_ReturnsInvalidParams covers the Task 8
// guard clause: an empty hosts list must be rejected before ever calling
// common.GenSelfSignedCert.
func TestGenSelfSignedCert_EmptyHosts_ReturnsInvalidParams(t *testing.T) {
	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	controller.GenSelfSignedCert(newGenSelfSignedCertContext(t, `{"hosts":[]}`))

	if gotCode != model.E_INVALID_PARAMS {
		t.Errorf("expected E_INVALID_PARAMS for empty hosts, got %q", gotCode)
	}
	if gotEntity == nil {
		t.Errorf("expected a non-nil error entity for empty hosts")
	}
}

// TestGenSelfSignedCert_ValidHosts_ReturnsPEM is the happy path: the handler
// only generates and returns cert/key PEM data, it must not touch the
// repository or persist anything (see task-8-brief.md design intent).
func TestGenSelfSignedCert_ValidHosts_ReturnsPEM(t *testing.T) {
	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	controller.GenSelfSignedCert(newGenSelfSignedCertContext(t, `{"hosts":["192.168.0.1","192.168.0.2"]}`))

	if gotCode != model.E_SUCCESS {
		t.Fatalf("expected E_SUCCESS, got %q (entity: %v)", gotCode, gotEntity)
	}
	rsp, ok := gotEntity.(model.SelfSignedCertRsp)
	if !ok {
		t.Fatalf("expected entity of type model.SelfSignedCertRsp, got %T", gotEntity)
	}
	if !strings.HasPrefix(rsp.Cert, "-----BEGIN CERTIFICATE-----") {
		t.Errorf("cert does not look like a PEM certificate: %q", rsp.Cert)
	}
	if !strings.Contains(rsp.Key, "PRIVATE KEY-----") {
		t.Errorf("key does not look like a PEM private key: %q", rsp.Key)
	}
}

// TestGenSelfSignedCert_MarksSkipResponseLog is the regression test for the
// final-review finding that GenSelfSignedCert's response - an unredacted
// plaintext private key - was landing verbatim in the Debug log via
// router.WrapMsg's unconditional log.Logger.Debugf. The handler now flags
// its gin.Context with "skip_response_log" so WrapMsg can omit the body;
// this test only verifies the flag is set (the WrapMsg-side branch that
// consumes it lives in package router and is covered by code walkthrough,
// see the final fix report).
func TestGenSelfSignedCert_MarksSkipResponseLog(t *testing.T) {
	var gotCode string
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, _ interface{}) {
		gotCode = retCode
	}}}

	c := newGenSelfSignedCertContext(t, `{"hosts":["192.168.0.1"]}`)
	controller.GenSelfSignedCert(c)

	if gotCode != model.E_SUCCESS {
		t.Fatalf("expected E_SUCCESS, got %q", gotCode)
	}
	if !c.GetBool("skip_response_log") {
		t.Errorf("expected GenSelfSignedCert to set skip_response_log=true on the context so the plaintext private key never reaches the Debug log")
	}
}

// TestCheckDeployParams_SecureRequiresValidCert is the regression test for
// Critical 1: checkDeployParams (the sole validator behind POST
// /api/v1/deploy/ck) used to contain zero references to Secure/TlsConf, so a
// brand-new cluster with Secure=true and no certificate sailed through
// validation and only failed once the full SSH deployment had already run
// and ClickHouse crashed on startup. The check must fire before any of the
// later package-registry/SSH-dependent validation, so both assertions below
// key off the specific tls error message rather than a bare err != nil -
// otherwise reverting the fix would still produce a (different, pkgtype-related)
// error and the test would pass for the wrong reason.
func TestCheckDeployParams_SecureRequiresValidCert(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	_, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	t.Run("missing TlsConf", func(t *testing.T) {
		conf := &model.CKManClickHouseConfig{Cluster: "deploytls-missing", Secure: true}
		err := checkDeployParams(conf, false)
		if err == nil {
			t.Fatalf("expected error when Secure=true and TlsConf is nil")
		}
		if !strings.Contains(err.Error(), "tls") {
			t.Errorf("expected a tls-specific validation error, got: %v", err)
		}
	})

	t.Run("mismatched cert/key", func(t *testing.T) {
		otherCertPEM, _, err := common.GenSelfSignedCert(nil)
		if err != nil {
			t.Fatalf("generate mismatched cert: %v", err)
		}
		conf := &model.CKManClickHouseConfig{
			Cluster: "deploytls-mismatch",
			Secure:  true,
			TlsConf: &model.TlsConf{Cert: otherCertPEM, Key: keyPEM},
		}
		err = checkDeployParams(conf, false)
		if err == nil {
			t.Fatalf("expected error when cert and key don't match")
		}
		if !strings.Contains(err.Error(), "tls certificate") {
			t.Errorf("expected an invalid-tls-certificate error, got: %v", err)
		}
	})
}

// TestCheckConfigParams_ImportModeSkipsTlsCheck and
// TestCheckConfigParams_DeployModeStillRequiresTls are the regression pair for
// Critical 2: imported clusters (Mode == model.CkClusterImport) are connect-only
// - ckman never holds their certificates - so Secure=true with TlsConf==nil is
// a legitimate permanent state that must not block every future config save.
// Non-import clusters must still be checked; splitting this into two tests
// guards against a fix that "solves" the bug by deleting the whole check
// (which would make DeployModeStillRequiresTls fail).
func TestCheckConfigParams_ImportModeSkipsTlsCheck(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	imported := model.CKManClickHouseConfig{
		Cluster: "tls-import-cluster",
		Mode:    model.CkClusterImport,
		Secure:  true,
		SshUser: "root",
		Port:    9000,
		// TlsConf intentionally nil: ckman doesn't manage certs for imported clusters.
	}
	if err := sp.CreateCluster(imported); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	conf := model.CKManClickHouseConfig{Cluster: "tls-import-cluster", Secure: true, SshUser: "root"}
	if err := checkConfigParams(&conf); err != nil {
		t.Errorf("expected an imported cluster with Secure=true and no TlsConf to pass checkConfigParams, got: %v", err)
	}
}

func TestCheckConfigParams_DeployModeStillRequiresTls(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	certPEM, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	deployed := baseTlsCluster(t, "tls-deploy-cluster", certPEM, keyPEM)
	deployed.Mode = model.CkClusterDeploy
	if err := sp.CreateCluster(deployed); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Submitted request omits TlsConf, same as the import-mode test above -
	// the only difference is Mode, which must make this one fail.
	conf := model.CKManClickHouseConfig{Cluster: "tls-deploy-cluster", Secure: true, SshUser: "root"}
	if err := checkConfigParams(&conf); err == nil {
		t.Errorf("expected a non-import cluster with Secure=true and no submitted TlsConf to fail checkConfigParams")
	}
}

// TestMergeClickhouseConfig_SecureDisabled_ClearsTlsConf is the regression
// test for Important 4: the design invariant "Secure=false must clear
// TlsConf" was never enforced server-side, relying entirely on the frontend
// not sending tlsConf back. This simulates a client that (whether by bug or
// by a stale cached form) still echoes the old certificate/key even though
// it flipped Secure off, and confirms both the in-memory merged config and
// the value actually written to the repository end up with TlsConf == nil.
func TestMergeClickhouseConfig_SecureDisabled_ClearsTlsConf(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	certPEM, keyPEM, err := common.GenSelfSignedCert(nil)
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	initial := baseTlsCluster(t, "tls-disable-test", certPEM, keyPEM)
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stored, err := sp.GetClusterbyName("tls-disable-test")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	submitted := stored
	submitted.Secure = false
	// Client still echoes the old cert/key even though Secure is being
	// turned off - the server must not trust this and must clear it anyway.
	submitted.TlsConf = &model.TlsConf{Cert: certPEM, Key: keyPEM, SelfSigned: true}

	_, _, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf("mergeClickhouseConfig returned error: %v", lerr)
	}
	if submitted.TlsConf != nil {
		t.Fatalf("expected TlsConf to be cleared in the merged config when Secure=false, got: %+v", submitted.TlsConf)
	}

	if err := sp.UpdateCluster(submitted); err != nil {
		t.Fatalf("update cluster: %v", err)
	}
	persisted, err := sp.GetClusterbyName("tls-disable-test")
	if err != nil {
		t.Fatalf("get cluster after update: %v", err)
	}
	if persisted.Secure {
		t.Errorf("expected persisted Secure=false")
	}
	if persisted.TlsConf != nil {
		t.Errorf("expected persisted TlsConf to be nil after disabling secure, got: %+v", persisted.TlsConf)
	}
}
