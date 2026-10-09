package wkafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	gcpGrantTypeTokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange"
	gcpTokenTypeAccessToken   = "urn:ietf:params:oauth:token-type:access_token"
	gcpTokenTypeMTLS          = "urn:ietf:params:oauth:token-type:mtls"
	gcpScopeCloudPlatform     = "https://www.googleapis.com/auth/cloud-platform"
	gcpDefaultTokenURL        = "https://sts.mtls.googleapis.com/v1/token"

	// GCPFormatRaw sends the Google access token as-is.
	GCPFormatRaw = "raw"
	// GCPFormatManagedKafka wraps the token in the GOOG_OAUTH2_TOKEN format
	// expected by Google Managed Service for Apache Kafka.
	GCPFormatManagedKafka = "managed_kafka"
)

// SaslOAuthGCP obtains Google Cloud access tokens with Workload Identity
// Federation using an X.509 client certificate (mTLS), optionally impersonating
// a service account.
//
// Every value can be given inline or as a file; inline and file forms of the
// same value cannot be combined. Cert, key and CA files are re-read on each
// token refresh so rotated certificates are picked up without restart.
type SaslOAuthGCP struct {
	Enabled bool `cfg:"enabled" json:"enabled"`

	// Credentials is the "external_account" JSON content. credential_source is ignored.
	Credentials     string `cfg:"credentials" json:"credentials" log:"false"`
	CredentialsFile string `cfg:"credentials_file" json:"credentials_file"`

	// Audience, TokenURL and ServiceAccountImpersonationURL override credentials.
	Audience                       string `cfg:"audience" json:"audience"`
	TokenURL                       string `cfg:"token_url" json:"token_url"`
	ServiceAccountImpersonationURL string `cfg:"service_account_impersonation_url" json:"service_account_impersonation_url"`

	// Cert is the PEM client certificate, chain allowed (leaf first).
	Cert     string `cfg:"cert" json:"cert"`
	CertFile string `cfg:"cert_file" json:"cert_file"`
	Key      string `cfg:"key" json:"key" log:"false"`
	KeyFile  string `cfg:"key_file" json:"key_file"`
	// CA is optional PEM roots for the Google endpoints; system roots if empty.
	CA     string `cfg:"ca" json:"ca"`
	CAFile string `cfg:"ca_file" json:"ca_file"`
	// InsecureSkipVerify disables server certificate and hostname verification. Unsafe for production.
	InsecureSkipVerify bool `cfg:"insecure_skip_verify" json:"insecure_skip_verify"`

	// Proxy is the URL of the proxy used for the Google token endpoints
	// (http, https, socks5 or socks5h). If empty, oauth.proxy is used, then
	// HTTPS_PROXY / HTTP_PROXY and NO_PROXY environment variables.
	Proxy string `cfg:"proxy" json:"proxy" log:"false"`

	// Scopes default to cloud-platform.
	Scopes []string `cfg:"scopes" json:"scopes"`
	// Lifetime of the impersonated token, default 1h.
	Lifetime time.Duration `cfg:"lifetime" json:"lifetime"`
	// RefreshBefore refreshes this long before expiry, default 5m.
	RefreshBefore time.Duration `cfg:"refresh_before" json:"refresh_before"`
	// Format is "raw" (default) or "managed_kafka".
	Format string `cfg:"format" json:"format"`
}

type gcpExternalAccount struct {
	Type                           string `json:"type"`
	Audience                       string `json:"audience"`
	SubjectTokenType               string `json:"subject_token_type"`
	TokenURL                       string `json:"token_url"`
	ServiceAccountImpersonationURL string `json:"service_account_impersonation_url"`
}

type gcpToken struct {
	accessToken string
	expiry      time.Time
}

type gcpTokenSource struct {
	cfg     SaslOAuthGCP
	timeout time.Duration
	now     func() time.Time

	mu     sync.Mutex
	mtls   *mtlsClient
	client *http.Client
	cached *gcpToken
}

func gcpReadFile(path string) (string, error) {
	v, err := readPEMFile(path)
	if err != nil {
		return "", fmt.Errorf("gcp: %w", err)
	}

	return v, nil
}

func newGCPTokenSource(c SaslOAuthGCP, timeout time.Duration) (*gcpTokenSource, error) {
	for _, p := range []struct {
		name         string
		inline, file string
	}{
		{"credentials", c.Credentials, c.CredentialsFile},
	} {
		if p.inline != "" && p.file != "" {
			return nil, fmt.Errorf("gcp: %s and %s_file cannot be used together", p.name, p.name)
		}
	}

	if c.CredentialsFile != "" {
		v, err := gcpReadFile(c.CredentialsFile)
		if err != nil {
			return nil, err
		}

		c.Credentials = v
	}

	if c.Credentials != "" {
		var ea gcpExternalAccount
		if err := json.Unmarshal([]byte(c.Credentials), &ea); err != nil {
			return nil, fmt.Errorf("gcp: parse credentials: %w", err)
		}

		if ea.Type != "" && ea.Type != "external_account" {
			return nil, fmt.Errorf("gcp: unsupported credentials type %q", ea.Type)
		}

		if ea.SubjectTokenType != "" && ea.SubjectTokenType != gcpTokenTypeMTLS {
			return nil, fmt.Errorf("gcp: unsupported subject_token_type %q, only %s", ea.SubjectTokenType, gcpTokenTypeMTLS)
		}

		c.Audience = gcpFirst(c.Audience, ea.Audience)
		c.TokenURL = gcpFirst(c.TokenURL, ea.TokenURL)
		c.ServiceAccountImpersonationURL = gcpFirst(c.ServiceAccountImpersonationURL, ea.ServiceAccountImpersonationURL)
	}

	if c.Audience == "" {
		return nil, errors.New("gcp: audience is required")
	}

	c.TokenURL = gcpFirst(c.TokenURL, gcpDefaultTokenURL)

	for _, u := range []string{c.TokenURL, c.ServiceAccountImpersonationURL} {
		if u == "" {
			continue
		}

		if p, err := url.Parse(u); err != nil || p.Host == "" || (p.Scheme != "https" && p.Scheme != "http") {
			return nil, fmt.Errorf("gcp: %q must be an absolute HTTP or HTTPS URL", u)
		}
	}

	if len(c.Scopes) == 0 {
		c.Scopes = []string{gcpScopeCloudPlatform}
	}

	if c.Lifetime < 0 || c.RefreshBefore < 0 {
		return nil, errors.New("gcp: lifetime and refresh_before must not be negative")
	}

	if c.Lifetime == 0 {
		c.Lifetime = time.Hour
	}

	if c.RefreshBefore == 0 {
		c.RefreshBefore = 5 * time.Minute
	}

	switch c.Format {
	case "":
		c.Format = GCPFormatRaw
	case GCPFormatRaw:
	case GCPFormatManagedKafka:
		if gcpServiceAccountEmail(c.ServiceAccountImpersonationURL) == "" {
			return nil, errors.New("gcp: managed_kafka format requires service_account_impersonation_url")
		}
	default:
		return nil, fmt.Errorf("gcp: unknown format %q", c.Format)
	}

	m, err := newMTLSClient(OAuthTLS{
		Cert: c.Cert, CertFile: c.CertFile,
		Key: c.Key, KeyFile: c.KeyFile,
		CA: c.CA, CAFile: c.CAFile,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}, c.Proxy)
	if err != nil {
		return nil, fmt.Errorf("gcp: %w", err)
	}

	if m.CertPEM() == "" {
		return nil, errors.New("gcp: cert and key are required")
	}

	s := &gcpTokenSource{cfg: c, timeout: timeout, now: time.Now, mtls: m}

	if _, err := s.subjectToken(); err != nil {
		return nil, err
	}

	return s, nil
}

func gcpFirst(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

// subjectToken is a JSON array of base64 DER certificates, leaf first.
func (s *gcpTokenSource) subjectToken() (string, error) {
	var certs []string

	rest := []byte(s.mtls.CertPEM())
	for {
		var block *pem.Block

		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type == "CERTIFICATE" {
			certs = append(certs, base64.StdEncoding.EncodeToString(block.Bytes))
		}
	}

	if len(certs) == 0 {
		return "", errors.New("gcp: no certificate found in cert")
	}

	b, err := json.Marshal(certs)

	return string(b), err
}

func gcpServiceAccountEmail(u string) string {
	const marker = "serviceAccounts/"

	i := strings.LastIndex(u, marker)
	if i < 0 {
		return ""
	}

	e := u[i+len(marker):]
	if j := strings.Index(e, ":"); j >= 0 {
		e = e[:j]
	}

	return e
}

// Token returns the formatted token, refreshing when close to expiry.
func (s *gcpTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	if s.cached == nil || !s.now().Add(s.cfg.RefreshBefore).Before(s.cached.expiry) {
		fetchCtx, cancel := context.WithTimeout(ctx, s.timeout)
		t, err := s.fetch(fetchCtx)

		cancel()

		switch {
		case err == nil:
			s.cached = t
		case s.cached != nil && s.now().Before(s.cached.expiry):
			slog.Warn("wkafka: gcp token refresh failed, using cached token", "error", err)
		default:
			return "", err
		}
	}

	return s.format(s.cached)
}

func (s *gcpTokenSource) fetch(ctx context.Context) (*gcpToken, error) {
	client, err := s.mtls.Client()
	if err != nil {
		return nil, fmt.Errorf("gcp: %w", err)
	}

	s.client = client

	subject, err := s.subjectToken()
	if err != nil {
		return nil, err
	}

	form := url.Values{
		"grant_type":           {gcpGrantTypeTokenExchange},
		"audience":             {s.cfg.Audience},
		"scope":                {strings.Join(s.cfg.Scopes, " ")},
		"requested_token_type": {gcpTokenTypeAccessToken},
		"subject_token":        {subject},
		"subject_token_type":   {gcpTokenTypeMTLS},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	var sts struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}

	if err := s.do(req, &sts); err != nil {
		return nil, fmt.Errorf("gcp: sts exchange: %w", err)
	}

	if sts.AccessToken == "" {
		return nil, errors.New("gcp: sts exchange: empty access_token")
	}

	if s.cfg.ServiceAccountImpersonationURL == "" {
		return &gcpToken{
			accessToken: sts.AccessToken,
			expiry:      s.now().Add(time.Duration(sts.ExpiresIn) * time.Second),
		}, nil
	}

	body, err := json.Marshal(map[string]any{
		"scope":    s.cfg.Scopes,
		"lifetime": strconv.FormatInt(int64(s.cfg.Lifetime.Seconds()), 10) + "s",
	})
	if err != nil {
		return nil, err
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.ServiceAccountImpersonationURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sts.AccessToken)

	var imp struct {
		AccessToken string    `json:"accessToken"`
		ExpireTime  time.Time `json:"expireTime"`
	}

	if err := s.do(req, &imp); err != nil {
		return nil, fmt.Errorf("gcp: impersonation: %w", err)
	}

	if imp.AccessToken == "" {
		return nil, errors.New("gcp: impersonation: empty accessToken")
	}

	return &gcpToken{accessToken: imp.AccessToken, expiry: imp.ExpireTime}, nil
}

func (s *gcpTokenSource) do(req *http.Request, v any) error {
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	return json.Unmarshal(b, v)
}

func (s *gcpTokenSource) format(t *gcpToken) (string, error) {
	if s.cfg.Format != GCPFormatManagedKafka {
		return t.accessToken, nil
	}

	enc := base64.RawURLEncoding

	header, err := json.Marshal(map[string]string{"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(map[string]any{
		"exp":   t.expiry.Unix(),
		"iat":   s.now().Unix(),
		"iss":   "Google",
		"scope": "kafka",
		"sub":   gcpServiceAccountEmail(s.cfg.ServiceAccountImpersonationURL),
	})
	if err != nil {
		return "", err
	}

	return enc.EncodeToString(header) + "." + enc.EncodeToString(payload) + "." +
		enc.EncodeToString([]byte(t.accessToken)), nil
}
