package controller

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

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

// ---------------------------------------------------------------------
// InspectCert / GenSelfSignedCert.NotAfter
//
// Root cause being fixed here: NotAfter is only ever populated by GetConfig
// (a read of an already-saved cluster, see the call to common.CertNotAfter
// a few lines into GetConfig above). Neither "import a certificate" nor
// "generate a self-signed certificate" on the frontend ever produced a
// NotAfter, so the expiry date silently failed to render right after
// import/generate - only reappearing once the user saved and reloaded.
// InspectCert closes the import-a-certificate gap; GenSelfSignedCert.NotAfter
// closes the generate-self-signed gap without an extra round trip.
// ---------------------------------------------------------------------

// genTestCertPEM builds a minimal self-signed certificate PEM with an
// arbitrary NotBefore/NotAfter window. It intentionally does not reuse
// common's unexported genSelfSignedCert helper (that lives in package
// common, this test lives in package controller) - InspectCert only ever
// looks at the certificate, never a private key, so a bare x509 cert is
// enough here.
func genTestCertPEM(t *testing.T, notBefore, notAfter time.Time) string {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "inspect-cert-test"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// newInspectCertContext mirrors newGenSelfSignedCertContext above, but for
// the /api/v1/ck/cert/inspect route.
func newInspectCertContext(t *testing.T, body string) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/ck/cert/inspect", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	return c
}

// TestInspectCert_ValidCert_ReturnsValidTrue is the happy path a "just
// imported a good certificate" click hits: Valid must be true and NotAfter
// must match a fresh CertNotAfter parse of the same cert.
func TestInspectCert_ValidCert_ReturnsValidTrue(t *testing.T) {
	certPEM, _, err := common.GenSelfSignedCert([]string{"192.168.0.1"})
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}
	wantNotAfter, err := common.CertNotAfter(certPEM)
	if err != nil {
		t.Fatalf("CertNotAfter: %v", err)
	}

	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	body, err := json.Marshal(model.CertInspectReq{Cert: certPEM})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	controller.InspectCert(newInspectCertContext(t, string(body)))

	if gotCode != model.E_SUCCESS {
		t.Fatalf("expected E_SUCCESS, got %q (entity: %v)", gotCode, gotEntity)
	}
	rsp, ok := gotEntity.(model.CertInspectRsp)
	if !ok {
		t.Fatalf("expected entity of type model.CertInspectRsp, got %T", gotEntity)
	}
	if !rsp.Valid {
		t.Errorf("expected Valid=true for a freshly generated, currently valid cert, got Valid=false (message: %q)", rsp.Message)
	}
	if rsp.NotAfter != wantNotAfter {
		t.Errorf("expected NotAfter=%q, got %q", wantNotAfter, rsp.NotAfter)
	}
	if rsp.Message != "" {
		t.Errorf("expected empty Message when Valid=true, got %q", rsp.Message)
	}
}

// TestInspectCert_ExpiredCert_ReturnsValidFalse covers "user imported a
// certificate that already expired": the endpoint must report Valid=false
// with a message that explains why, at import time rather than only once
// the user tries to save (see checkConfigParams/ValidateCertKeyPair).
func TestInspectCert_ExpiredCert_ReturnsValidFalse(t *testing.T) {
	now := time.Now()
	certPEM := genTestCertPEM(t, now.AddDate(-2, 0, 0), now.AddDate(-1, 0, 0))

	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	body, err := json.Marshal(model.CertInspectReq{Cert: certPEM})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	controller.InspectCert(newInspectCertContext(t, string(body)))

	if gotCode != model.E_SUCCESS {
		t.Fatalf("expected E_SUCCESS (a parseable-but-expired cert is a business result, not a request error), got %q", gotCode)
	}
	rsp, ok := gotEntity.(model.CertInspectRsp)
	if !ok {
		t.Fatalf("expected entity of type model.CertInspectRsp, got %T", gotEntity)
	}
	if rsp.Valid {
		t.Errorf("expected Valid=false for an expired certificate")
	}
	if !strings.Contains(rsp.Message, "expired") {
		t.Errorf("expected Message to explain the certificate expired, got %q", rsp.Message)
	}
	if rsp.NotAfter == "" {
		t.Errorf("expected NotAfter to still be populated for an expired-but-parseable cert")
	}
}

// TestInspectCert_GarbageInput_ReturnsValidFalseNoPanic covers pasting
// something that isn't a PEM certificate at all: must not panic, and must
// be reported as a business-level Valid=false rather than a request error,
// since from the frontend's perspective this is just "not a valid cert yet".
func TestInspectCert_GarbageInput_ReturnsValidFalseNoPanic(t *testing.T) {
	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	body, err := json.Marshal(model.CertInspectReq{Cert: "not a pem certificate at all"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("InspectCert panicked on garbage input: %v", r)
		}
	}()
	controller.InspectCert(newInspectCertContext(t, string(body)))

	if gotCode != model.E_SUCCESS {
		t.Fatalf("expected E_SUCCESS, got %q", gotCode)
	}
	rsp, ok := gotEntity.(model.CertInspectRsp)
	if !ok {
		t.Fatalf("expected entity of type model.CertInspectRsp, got %T", gotEntity)
	}
	if rsp.Valid {
		t.Errorf("expected Valid=false for unparseable input")
	}
	if rsp.Message == "" {
		t.Errorf("expected a non-empty Message explaining the parse failure")
	}
	if rsp.NotAfter != "" {
		t.Errorf("expected NotAfter to stay empty when the certificate could not be parsed, got %q", rsp.NotAfter)
	}
}

// TestInspectCert_EmptyCert_ReturnsInvalidParams: an empty cert field is a
// malformed request, not a "certificate that failed validation" - it must
// be rejected before any parsing is attempted.
func TestInspectCert_EmptyCert_ReturnsInvalidParams(t *testing.T) {
	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	controller.InspectCert(newInspectCertContext(t, `{"cert":""}`))

	if gotCode != model.E_INVALID_PARAMS {
		t.Errorf("expected E_INVALID_PARAMS for empty cert, got %q", gotCode)
	}
	if gotEntity == nil {
		t.Errorf("expected a non-nil error entity for empty cert")
	}
}

// TestGenSelfSignedCert_ReturnsNotAfter is the regression test for the
// generate-self-signed half of the reported bug: right after generating a
// certificate, the frontend has nowhere else to get an expiry date from
// (GetConfig only fires on an already-saved cluster), so the response
// itself must carry a correctly formatted NotAfter.
func TestGenSelfSignedCert_ReturnsNotAfter(t *testing.T) {
	var gotCode string
	var gotEntity interface{}
	controller := &ClickHouseController{Controller: Controller{wrapfunc: func(_ *gin.Context, retCode string, entity interface{}) {
		gotCode = retCode
		gotEntity = entity
	}}}

	controller.GenSelfSignedCert(newGenSelfSignedCertContext(t, `{"hosts":["192.168.0.1"]}`))

	if gotCode != model.E_SUCCESS {
		t.Fatalf("expected E_SUCCESS, got %q (entity: %v)", gotCode, gotEntity)
	}
	rsp, ok := gotEntity.(model.SelfSignedCertRsp)
	if !ok {
		t.Fatalf("expected entity of type model.SelfSignedCertRsp, got %T", gotEntity)
	}
	if rsp.NotAfter == "" {
		t.Fatalf("expected NotAfter to be populated so the frontend doesn't need a second inspect call")
	}
	wantNotAfter, err := common.CertNotAfter(rsp.Cert)
	if err != nil {
		t.Fatalf("CertNotAfter on returned cert: %v", err)
	}
	if rsp.NotAfter != wantNotAfter {
		t.Errorf("NotAfter=%q does not match a fresh parse of the returned cert (%q)", rsp.NotAfter, wantNotAfter)
	}
	if _, err := time.Parse("2006-01-02 15:04:05", rsp.NotAfter); err != nil {
		t.Errorf("NotAfter %q is not in the expected layout: %v", rsp.NotAfter, err)
	}
}

// ---------------------------------------------------------------------
// Protocol/HttpPort port-protocol-confusion regression suite.
//
// Root cause: a real cluster was switched Secure=false -> true (Port
// 9000 -> 9440) while HttpPort stayed pinned at 8123, because both
// HttpPort and Protocol were Editable:"false" on the config-update
// schema and mergeClickhouseConfig never compared HttpPort/Protocol at
// all - so even a request that did carry a new HttpPort silently lost
// it (overwritten by the untouched cluster.HttpPort at the end of the
// merge). ClickHouse ended up serving HTTPS on the port everyone
// assumes is plaintext.
// ---------------------------------------------------------------------

// schemaFieldKey mirrors the "<pkgpath>.<TypeName>.<Field>" key format
// ConfigParams.MustRegister uses internally (see common/config.go).
func schemaFieldKey(v interface{}, field string) string {
	rt := reflect.TypeOf(v)
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt.PkgPath() + "." + rt.Name() + "." + field
}

// TestRegistUpdateConfigSchema_ProtocolAndHttpPortEditable is the regression
// test for the config page half of the bug: Protocol and HttpPort were both
// locked (Editable:"false"), so a cluster stuck with the wrong HttpPort after
// a Secure flip could never be corrected from the config page - the only
// path that changed Port (native) worked, HttpPort did not. Protocol in
// particular carries zero deployment risk: it only selects which
// protocol/port ckman itself dials the cluster with.
func TestRegistUpdateConfigSchema_ProtocolAndHttpPortEditable(t *testing.T) {
	params := RegistUpdateConfigSchema()
	var conf model.CKManClickHouseConfig

	protocolParam, ok := params[schemaFieldKey(conf, "Protocol")]
	if !ok {
		t.Fatalf("Protocol is not registered in the update-config schema")
	}
	if protocolParam.Editable == "false" {
		t.Errorf("Protocol must be editable on the config-update page: changing it never touches the ClickHouse server side")
	}

	httpPortParam, ok := params[schemaFieldKey(conf, "HttpPort")]
	if !ok {
		t.Fatalf("HttpPort is not registered in the update-config schema")
	}
	if httpPortParam.Editable == "false" {
		t.Errorf("HttpPort must be editable on the config-update page, otherwise a cluster that switches Secure on can never move its HTTP port off the plaintext default")
	}
}

// baseNonTlsCluster returns a minimal plaintext CKManClickHouseConfig for the
// merge-change-detection tests below, where TLS itself is not what's under
// test.
func baseNonTlsCluster(name string) model.CKManClickHouseConfig {
	return model.CKManClickHouseConfig{
		Cluster:  name,
		Comment:  "original comment",
		Port:     model.ClickHouseDefaultPort,
		HttpPort: model.ClickHouseDefaultHttpPort,
		Protocol: "native",
		Secure:   false,
		SshUser:  "root",
	}
}

// TestMergeClickhouseConfig_HttpPortChange_TriggersConfigPushAndRestart is
// the regression test for Fix 2: mergeClickhouseConfig used to compare Port
// but never HttpPort, so a request that changed only HttpPort both (a) never
// triggered cc/restart and (b) had its new value silently discarded - the
// final `common.DeepCopyByGob(conf, cluster)` copies the *unmerged* cluster
// state back over conf, and cluster.HttpPort was never reassigned from
// conf.HttpPort. The custom port used here (18123) deliberately avoids both
// the 8123/8443 plaintext/secure defaults so it isn't touched by
// NormalizeSecurePorts, keeping this test focused on HttpPort-diff detection
// alone.
func TestMergeClickhouseConfig_HttpPortChange_TriggersConfigPushAndRestart(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	initial := baseNonTlsCluster("httpporttest")
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stored, err := sp.GetClusterbyName("httpporttest")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	submitted := stored
	submitted.HttpPort = 18123

	restart, cc, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf("mergeClickhouseConfig returned error: %v", lerr)
	}
	if !cc {
		t.Errorf("cc=false: changing HttpPort alone must trigger a config re-push, it feeds custom.xml's http_port/https_port directly")
	}
	if !restart {
		t.Errorf("restart=false: changing HttpPort alone must trigger a cluster restart")
	}
	if submitted.HttpPort != 18123 {
		t.Errorf("expected the merged config to keep the new HttpPort 18123, got %d (it must not be silently discarded)", submitted.HttpPort)
	}
}

// TestMergeClickhouseConfig_ProtocolOnlyChange_PersistsWithoutRestartOrConfigPush
// is the regression test for the other half of Fix 2: Protocol was entirely
// absent from noChangedFn, so a request that changed nothing but Protocol
// was rejected outright with "all config are the same, it's no need to
// update" - the save could never go through. Once accepted, Protocol must
// not force a config re-push or restart: it never affects anything written
// to a ClickHouse node.
func TestMergeClickhouseConfig_ProtocolOnlyChange_PersistsWithoutRestartOrConfigPush(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	initial := baseNonTlsCluster("protocolonlytest")
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stored, err := sp.GetClusterbyName("protocolonlytest")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	submitted := stored
	submitted.Protocol = "http"

	restart, cc, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf(`mergeClickhouseConfig returned an error for a Protocol-only change (must not be treated as "nothing changed"): %v`, lerr)
	}
	if restart {
		t.Errorf("restart=true: Protocol only changes which protocol ckman itself dials with, it must never trigger a cluster restart")
	}
	if cc {
		t.Errorf("cc=true: a Protocol-only change must not trigger a config re-push, nothing on the ClickHouse server side depends on it")
	}
	if submitted.Protocol != "http" {
		t.Errorf("expected the merged config to keep the new Protocol %q, got %q", "http", submitted.Protocol)
	}
}

// TestMergeClickhouseConfig_SecureEnabled_AutoSwitchesDefaultPorts is the
// regression test for Fix 3 on the ClusterSetting save path: this is exactly
// the field incident - a request flips Secure on but the frontend (or a
// direct API caller, or a scripted deploy) left Port/HttpPort sitting at
// the plaintext defaults. The backend must not trust that and must switch
// them to the TLS defaults itself.
func TestMergeClickhouseConfig_SecureEnabled_AutoSwitchesDefaultPorts(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	initial := baseNonTlsCluster("secureautoswitch")
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stored, err := sp.GetClusterbyName("secureautoswitch")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	submitted := stored
	submitted.Secure = true
	// Port/HttpPort deliberately left untouched at the plaintext defaults,
	// simulating the frontend's Secure-linked port switcher not having run.

	restart, cc, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf("mergeClickhouseConfig returned error: %v", lerr)
	}
	if submitted.Port != model.ClickHouseDefaultSecurePort {
		t.Errorf("expected Port to be auto-switched to %d when Secure is enabled with the port still at its plaintext default, got %d", model.ClickHouseDefaultSecurePort, submitted.Port)
	}
	if submitted.HttpPort != model.ClickHouseDefaultSecureHttpPort {
		t.Errorf("expected HttpPort to be auto-switched to %d when Secure is enabled with the port still at its plaintext default, got %d", model.ClickHouseDefaultSecureHttpPort, submitted.HttpPort)
	}
	if !cc || !restart {
		t.Errorf("expected cc=true and restart=true when Secure changes, got cc=%v restart=%v", cc, restart)
	}
}

// TestMergeClickhouseConfig_SecureEnabled_PreservesCustomPorts is the
// counterpart negative test: a user who deliberately runs ClickHouse behind
// a non-default port (e.g. 9001) must not have that choice silently
// overridden just because they also flipped Secure on.
func TestMergeClickhouseConfig_SecureEnabled_PreservesCustomPorts(t *testing.T) {
	sp := newTestPersistent(t)
	repository.Ps = sp

	initial := baseNonTlsCluster("securecustomports")
	initial.Port = 9001 // deliberate, non-default
	if err := sp.CreateCluster(initial); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stored, err := sp.GetClusterbyName("securecustomports")
	if err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	submitted := stored
	submitted.Secure = true

	_, _, _, lerr := mergeClickhouseConfig(&submitted, false)
	if lerr != nil {
		t.Fatalf("mergeClickhouseConfig returned error: %v", lerr)
	}
	if submitted.Port != 9001 {
		t.Errorf("expected the user's custom Port 9001 to survive the Secure flip untouched, got %d", submitted.Port)
	}
	// HttpPort was left at its plaintext default (8123) and must still switch.
	if submitted.HttpPort != model.ClickHouseDefaultSecureHttpPort {
		t.Errorf("expected HttpPort still at its default to switch to %d, got %d", model.ClickHouseDefaultSecureHttpPort, submitted.HttpPort)
	}
}
