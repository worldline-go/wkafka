package wkafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const gcpTestSA = "sa@proj.iam.gserviceaccount.com"

func gcpGenCert(t *testing.T) (string, string, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})), cert
}

type gcpFake struct {
	srv      *httptest.Server
	sts, imp atomic.Int32
	fail     atomic.Bool
	expires  time.Time
}

func newGCPFake(t *testing.T, client *x509.Certificate) *gcpFake {
	t.Helper()
	f := &gcpFake{expires: time.Now().Add(time.Hour)}
	pool := x509.NewCertPool()
	pool.AddCert(client)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		f.sts.Add(1)
		if f.fail.Load() {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		var certs []string
		if r.FormValue("subject_token_type") != gcpTokenTypeMTLS ||
			r.FormValue("grant_type") != gcpGrantTypeTokenExchange ||
			r.FormValue("audience") != "//iam.googleapis.com/test" ||
			json.Unmarshal([]byte(r.FormValue("subject_token")), &certs) != nil || len(certs) != 1 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		der, _ := base64.StdEncoding.DecodeString(certs[0])
		if string(r.TLS.PeerCertificates[0].Raw) != string(der) {
			http.Error(w, "cert mismatch", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "sts-token", "expires_in": 3600})
	})
	mux.HandleFunc("/v1/projects/-/serviceAccounts/"+gcpTestSA+":generateAccessToken", func(w http.ResponseWriter, r *http.Request) {
		f.imp.Add(1)
		var body struct {
			Lifetime string `json:"lifetime"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Header.Get("Authorization") != "Bearer sts-token" || body.Lifetime != "3600s" {
			http.Error(w, "bad impersonation", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"accessToken": "ya29.final",
			"expireTime":  f.expires.UTC().Format(time.RFC3339),
		})
	})

	f.srv = httptest.NewUnstartedServer(mux)
	f.srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	f.srv.StartTLS()
	t.Cleanup(f.srv.Close)
	return f
}

func gcpTestConfig(t *testing.T) (SaslOAuthGCP, *gcpFake) {
	t.Helper()
	certPEM, keyPEM, cert := gcpGenCert(t)
	f := newGCPFake(t, cert)
	creds, err := json.Marshal(map[string]any{
		"type":                              "external_account",
		"audience":                          "//iam.googleapis.com/test",
		"subject_token_type":                gcpTokenTypeMTLS,
		"token_url":                         f.srv.URL + "/v1/token",
		"service_account_impersonation_url": f.srv.URL + "/v1/projects/-/serviceAccounts/" + gcpTestSA + ":generateAccessToken",
	})
	require.NoError(t, err)
	return SaslOAuthGCP{
		Enabled:     true,
		Credentials: string(creds),
		Cert:        certPEM,
		Key:         keyPEM,
		CA:          string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.srv.Certificate().Raw})),
	}, f
}

func TestOAuthGCPMechanismCached(t *testing.T) {
	cfg, f := gcpTestConfig(t)
	m, err := (SaslOAuth{Enabled: true, GCP: cfg}).Generate()
	require.NoError(t, err)
	require.Equal(t, "OAUTHBEARER", m.Name())

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, initial, err := m.Authenticate(t.Context(), "broker:9092")
			if err != nil {
				t.Error(err)
				return
			}
			if !strings.Contains(string(initial), "auth=Bearer ya29.final") {
				t.Errorf("unexpected initial %q", initial)
			}
		})
	}
	wg.Wait()
	require.EqualValues(t, 1, f.sts.Load())
	require.EqualValues(t, 1, f.imp.Load())
}

func TestOAuthGCPManagedKafkaFormat(t *testing.T) {
	cfg, _ := gcpTestConfig(t)
	cfg.Format = GCPFormatManagedKafka
	s, err := newGCPTokenSource(cfg, time.Second)
	require.NoError(t, err)
	token, err := s.Token(t.Context())
	require.NoError(t, err)
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	require.Contains(t, string(payload), `"sub":"`+gcpTestSA+`"`)
	require.Equal(t, "ya29.final", string(sig))
}

func TestOAuthGCPStaleOnFailure(t *testing.T) {
	cfg, f := gcpTestConfig(t)
	s, err := newGCPTokenSource(cfg, time.Second)
	require.NoError(t, err)
	_, err = s.Token(t.Context())
	require.NoError(t, err)

	f.fail.Store(true)
	s.now = func() time.Time { return f.expires.Add(-time.Minute) }
	token, err := s.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "ya29.final", token)

	s.now = func() time.Time { return f.expires.Add(time.Minute) }
	_, err = s.Token(t.Context())
	require.ErrorContains(t, err, "sts exchange")
}

func TestOAuthGCPFilesAndRotation(t *testing.T) {
	cfg, f := gcpTestConfig(t)
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
		return p
	}
	cfg.CredentialsFile = write("creds.json", cfg.Credentials)
	cfg.CertFile = write("cert.pem", cfg.Cert)
	cfg.KeyFile = write("key.pem", cfg.Key)
	cfg.CAFile = write("ca.pem", cfg.CA)
	cfg.Credentials, cfg.Cert, cfg.Key, cfg.CA = "", "", "", ""

	s, err := newGCPTokenSource(cfg, time.Second)
	require.NoError(t, err)
	_, err = s.Token(t.Context())
	require.NoError(t, err)

	// rotate to an untrusted cert: the next refresh must pick it up and fail the handshake
	certPEM, keyPEM, _ := gcpGenCert(t)
	require.NoError(t, os.WriteFile(cfg.CertFile, []byte(certPEM), 0o600))
	require.NoError(t, os.WriteFile(cfg.KeyFile, []byte(keyPEM), 0o600))
	s.cached = nil
	before := f.sts.Load()
	_, err = s.Token(t.Context())
	require.Error(t, err)
	require.Equal(t, before, f.sts.Load())
}

func TestOAuthGCPValidation(t *testing.T) {
	for name, c := range map[string]SaslOAuth{
		"gcp and token":   {Enabled: true, Token: "x", GCP: SaslOAuthGCP{Enabled: true}},
		"no audience":     {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Cert: "x", Key: "y"}},
		"cert both":       {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Audience: "a", Cert: "x", CertFile: "/x", Key: "y"}},
		"missing file":    {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Audience: "a", CertFile: "/does/not/exist", Key: "y"}},
		"bad type":        {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Credentials: `{"type":"service_account"}`}},
		"bad subject":     {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Credentials: `{"audience":"a","subject_token_type":"urn:ietf:params:oauth:token-type:jwt"}`}},
		"bad format":      {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Audience: "a", Format: "nope"}},
		"kafka no sa url": {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Audience: "a", Format: GCPFormatManagedKafka}},
		"no cert":         {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Audience: "a"}},
		"bad proxy":       {Enabled: true, GCP: SaslOAuthGCP{Enabled: true, Audience: "a", Cert: "x", Key: "y", Proxy: "ftp://p:1"}},
	} {
		_, err := c.Generate()
		require.Error(t, err, name)
	}
}

func TestOAuthGCPProxy(t *testing.T) {
	cfg, f := gcpTestConfig(t)

	var connects atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "connect only", http.StatusMethodNotAllowed)
			return
		}
		connects.Add(1)
		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			upstream.Close()
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))
		go func() {
			_, _ = io.Copy(upstream, conn)
			upstream.Close()
		}()
		_, _ = io.Copy(conn, upstream)
		conn.Close()
	}))
	t.Cleanup(proxy.Close)

	cfg.Proxy = proxy.URL
	s, err := newGCPTokenSource(cfg, 5*time.Second)
	require.NoError(t, err)
	token, err := s.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "ya29.final", token)
	require.Positive(t, connects.Load())
	require.EqualValues(t, 1, f.sts.Load())
}
