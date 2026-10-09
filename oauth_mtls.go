package wkafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

// OAuthTLS is the client certificate used to authenticate to a token endpoint
// (mutual TLS). Every value can be given inline (PEM) or as a file; inline and
// file forms of the same value cannot be combined. Files are re-read on each
// token refresh so rotated certificates are picked up without restart.
type OAuthTLS struct {
	// InsecureSkipVerify disables server certificate and hostname verification. Unsafe for production.
	InsecureSkipVerify bool `cfg:"insecure_skip_verify" json:"insecure_skip_verify"`
	// Cert is the PEM client certificate, chain allowed (leaf first).
	Cert     string `cfg:"cert" json:"cert"`
	CertFile string `cfg:"cert_file" json:"cert_file"`
	Key      string `cfg:"key" json:"key" log:"false"`
	KeyFile  string `cfg:"key_file" json:"key_file"`
	// CA is optional PEM roots for the token endpoint; system roots if empty.
	CA     string `cfg:"ca" json:"ca"`
	CAFile string `cfg:"ca_file" json:"ca_file"`
}

// IsSet reports whether any TLS option is configured.
func (t OAuthTLS) IsSet() bool {
	return t.InsecureSkipVerify || t.Cert != "" || t.CertFile != "" || t.Key != "" || t.KeyFile != "" || t.CA != "" || t.CAFile != ""
}

// mtlsClient builds an HTTP client from OAuthTLS and rebuilds it when the
// certificate files change. It is not safe for concurrent use; callers
// serialize access.
type mtlsClient struct {
	cfg    OAuthTLS
	proxy  *url.URL
	client *http.Client
}

func readPEMFile(path string) (string, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// newMTLSClient builds the client. An empty proxy uses the HTTP(S)_PROXY and
// NO_PROXY environment variables.
func newMTLSClient(cfg OAuthTLS, proxy string) (*mtlsClient, error) {
	for _, p := range []struct {
		name         string
		inline, file string
	}{
		{"cert", cfg.Cert, cfg.CertFile},
		{"key", cfg.Key, cfg.KeyFile},
		{"ca", cfg.CA, cfg.CAFile},
	} {
		if p.inline != "" && p.file != "" {
			return nil, fmt.Errorf("%s and %s_file cannot be used together", p.name, p.name)
		}
	}

	m := &mtlsClient{cfg: cfg}

	if proxy != "" {
		u, err := url.Parse(proxy)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
			return nil, errors.New("proxy must be an absolute http, https, socks5 or socks5h URL")
		}

		m.proxy = u
	}

	if _, err := m.reload(); err != nil {
		return nil, err
	}

	if (m.cfg.Cert == "") != (m.cfg.Key == "") {
		return nil, errors.New("cert and key must be provided together")
	}

	if err := m.build(); err != nil {
		return nil, err
	}

	return m, nil
}

// reload re-reads configured files and reports whether content changed.
func (m *mtlsClient) reload() (bool, error) {
	changed := false

	for _, f := range []struct {
		path string
		dst  *string
	}{
		{m.cfg.CertFile, &m.cfg.Cert},
		{m.cfg.KeyFile, &m.cfg.Key},
		{m.cfg.CAFile, &m.cfg.CA},
	} {
		if f.path == "" {
			continue
		}

		v, err := readPEMFile(f.path)
		if err != nil {
			return false, err
		}

		if v != *f.dst {
			*f.dst = v
			changed = true
		}
	}

	return changed, nil
}

func (m *mtlsClient) build() error {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: m.cfg.InsecureSkipVerify}

	if m.cfg.Cert != "" {
		cert, err := tls.X509KeyPair([]byte(m.cfg.Cert), []byte(m.cfg.Key))
		if err != nil {
			return fmt.Errorf("load cert/key: %w", err)
		}

		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	if m.cfg.CA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(m.cfg.CA)) {
			return errors.New("no valid certificate in ca")
		}

		tlsCfg.RootCAs = pool
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsCfg

	if m.proxy != nil {
		tr.Proxy = http.ProxyURL(m.proxy)
	}

	if m.client != nil {
		m.client.CloseIdleConnections()
	}

	m.client = &http.Client{Transport: tr}

	return nil
}

// Client returns the current HTTP client, rebuilding it if files changed.
func (m *mtlsClient) Client() (*http.Client, error) {
	changed, err := m.reload()
	if err != nil {
		return nil, err
	}

	if changed {
		if err := m.build(); err != nil {
			return nil, err
		}
	}

	return m.client, nil
}

// CertPEM returns the current client certificate PEM.
func (m *mtlsClient) CertPEM() string {
	return m.cfg.Cert
}
