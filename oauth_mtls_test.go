package wkafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type mtlsTokenServer struct {
	srv   *httptest.Server
	calls atomic.Int32
	caPEM string
}

func TestOAuthTLSInsecureSkipVerify(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(s.Close)
	for _, insecure := range []bool{false, true} {
		cfg := OAuthTLS{InsecureSkipVerify: insecure}
		require.Equal(t, insecure, cfg.IsSet())
		m, err := (SaslOAuth{
			Enabled: true, TokenURL: s.URL, ClientID: "worker", ClientSecret: "secret", TLS: cfg,
		}).Generate()
		require.NoError(t, err)
		_, initial, err := m.Authenticate(t.Context(), "broker:9092")
		if insecure {
			require.NoError(t, err)
			require.Contains(t, string(initial), "auth=Bearer test-token")
		} else {
			require.Error(t, err)
		}
	}
}

func newMTLSTokenServer(t *testing.T, client *x509.Certificate, wantSecret string) *mtlsTokenServer {
	t.Helper()
	s := &mtlsTokenServer{}
	pool := x509.NewCertPool()
	pool.AddCert(client)
	s.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, basic := r.BasicAuth()
		ok := r.FormValue("grant_type") == "client_credentials"
		if wantSecret == "" {
			ok = ok && !basic && r.FormValue("client_id") == "worker" && r.FormValue("client_secret") == ""
		} else {
			ok = ok && basic && user == "worker" && pass == wantSecret
		}
		if !ok {
			http.Error(w, `{"error":"invalid_client"}`, http.StatusUnauthorized)
			return
		}
		n := s.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"mtls-%d","token_type":"Bearer","expires_in":3600}`, n)
	}))
	s.srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	s.srv.StartTLS()
	t.Cleanup(s.srv.Close)
	s.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.srv.Certificate().Raw}))
	return s
}

func TestOAuthClientCredentialsMTLS(t *testing.T) {
	for _, secret := range []string{"", "secret"} {
		t.Run("secret="+secret, func(t *testing.T) {
			certPEM, keyPEM, cert := gcpGenCert(t)
			s := newMTLSTokenServer(t, cert, secret)
			m, err := (SaslOAuth{
				Enabled: true, TokenURL: s.srv.URL, ClientID: "worker", ClientSecret: secret,
				TLS: OAuthTLS{Cert: certPEM, Key: keyPEM, CA: s.caPEM},
			}).Generate()
			require.NoError(t, err)
			for range 3 {
				_, initial, err := m.Authenticate(t.Context(), "broker:9092")
				require.NoError(t, err)
				require.Contains(t, string(initial), "auth=Bearer mtls-1")
			}
			require.EqualValues(t, 1, s.calls.Load())
		})
	}
}

func TestOAuthClientCredentialsMTLSRequiresCert(t *testing.T) {
	_, _, cert := gcpGenCert(t)
	s := newMTLSTokenServer(t, cert, "secret")
	// CA only: secret still works for basic auth but handshake fails without a client cert.
	m, err := (SaslOAuth{
		Enabled: true, TokenURL: s.srv.URL, ClientID: "worker", ClientSecret: "secret",
		TLS: OAuthTLS{CA: s.caPEM},
	}).Generate()
	require.NoError(t, err)
	_, _, err = m.Authenticate(t.Context(), "broker:9092")
	require.Error(t, err)
	require.EqualValues(t, 0, s.calls.Load())
}

func TestOAuthClientCredentialsMTLSFileRotation(t *testing.T) {
	certPEM, keyPEM, cert := gcpGenCert(t)
	s := newMTLSTokenServer(t, cert, "")
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
		return p
	}
	tlsCfg := OAuthTLS{CertFile: write("cert.pem", certPEM), KeyFile: write("key.pem", keyPEM), CAFile: write("ca.pem", s.caPEM)}

	m, err := newMTLSClient(tlsCfg, "")
	require.NoError(t, err)
	c1, err := m.Client()
	require.NoError(t, err)
	c2, err := m.Client()
	require.NoError(t, err)
	require.Same(t, c1, c2, "unchanged files must reuse client")

	newCert, newKey, _ := gcpGenCert(t)
	require.NoError(t, os.WriteFile(tlsCfg.CertFile, []byte(newCert), 0o600))
	require.NoError(t, os.WriteFile(tlsCfg.KeyFile, []byte(newKey), 0o600))
	c3, err := m.Client()
	require.NoError(t, err)
	require.NotSame(t, c1, c3, "rotated files must rebuild client")
	require.Equal(t, newCert, m.CertPEM())
}

func TestOAuthClientCredentialsMTLSValidation(t *testing.T) {
	certPEM, keyPEM, _ := gcpGenCert(t)
	for name, c := range map[string]SaslOAuth{
		"no secret no cert": {Enabled: true, TokenURL: "https://x", ClientID: "c", TLS: OAuthTLS{CA: "x"}},
		"cert without key":  {Enabled: true, TokenURL: "https://x", ClientID: "c", TLS: OAuthTLS{Cert: certPEM}},
		"cert both":         {Enabled: true, TokenURL: "https://x", ClientID: "c", TLS: OAuthTLS{Cert: certPEM, CertFile: "/x", Key: keyPEM}},
		"missing file":      {Enabled: true, TokenURL: "https://x", ClientID: "c", TLS: OAuthTLS{CertFile: "/does/not/exist", Key: keyPEM}},
		"bad pair":          {Enabled: true, TokenURL: "https://x", ClientID: "c", TLS: OAuthTLS{Cert: certPEM, Key: "nope"}},
		"tls with token":    {Enabled: true, Token: "t", TLS: OAuthTLS{Cert: certPEM, Key: keyPEM}},
		"no client id":      {Enabled: true, TokenURL: "https://x", TLS: OAuthTLS{Cert: certPEM, Key: keyPEM}},
	} {
		_, err := c.Generate()
		require.Error(t, err, name)
	}
}
