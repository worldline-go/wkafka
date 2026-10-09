package wkafka

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// SaslOAuth configures OAUTHBEARER using exactly one of a static token,
// client credentials (optionally with mutual TLS), Google Cloud workload
// identity federation, or a programmatic provider.
type SaslOAuth struct {
	Enabled      bool              `cfg:"enabled" json:"enabled"`
	Zid          string            `cfg:"zid" json:"zid"`
	Extensions   map[string]string `cfg:"extensions" json:"extensions"`
	Token        string            `cfg:"token" json:"token" log:"false"`
	TokenURL     string            `cfg:"token_url" json:"token_url"`
	ClientID     string            `cfg:"client_id" json:"client_id"`
	ClientSecret string            `cfg:"client_secret" json:"client_secret" log:"false"`
	Scopes       []string          `cfg:"scopes" json:"scopes"`
	// TLS configures the client certificate and CA used for the token endpoint
	// in client credentials mode. With a client certificate, client_secret is
	// optional (RFC 8705 tls_client_auth).
	TLS OAuthTLS `cfg:"tls" json:"tls"`
	// Proxy is the URL of the proxy used for the token endpoint (http, https,
	// socks5 or socks5h). If empty, HTTPS_PROXY / HTTP_PROXY and NO_PROXY
	// environment variables are used. GCP uses it when gcp.proxy is empty.
	Proxy string `cfg:"proxy" json:"proxy" log:"false"`
	// Timeout bounds token endpoint requests; zero defaults to 10 seconds.
	Timeout time.Duration `cfg:"timeout" json:"timeout"`
	// TokenProvider is called for each authentication and owns token caching
	// and renewal. It must be safe for concurrent use. Its Auth is used as-is,
	// including Zid and Extensions, rather than merged with the config fields.
	TokenProvider func(context.Context) (oauth.Auth, error) `cfg:"-" json:"-" log:"false"`
	// GCP fetches tokens from Google Cloud with X.509 workload identity federation.
	GCP SaslOAuthGCP `cfg:"gcp" json:"gcp"`
}

func (s SaslOAuth) Generate() (sasl.Mechanism, error) {
	if !s.Enabled {
		return nil, nil
	}
	credentials := s.TokenURL != "" || s.ClientID != "" || s.ClientSecret != "" || len(s.Scopes) > 0 || s.TLS.IsSet()
	modes := 0
	for _, set := range []bool{s.Token != "", s.TokenProvider != nil, credentials, s.GCP.Enabled} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return nil, fmt.Errorf("oauth: configure exactly one of token, client credentials, gcp, or token provider")
	}
	if s.Timeout < 0 {
		return nil, fmt.Errorf("oauth: timeout must not be negative")
	}
	if s.TokenProvider != nil {
		return oauth.Oauth(s.TokenProvider), nil
	}
	extensions := maps.Clone(s.Extensions)
	if s.GCP.Enabled {
		timeout := s.Timeout
		if timeout == 0 {
			timeout = 10 * time.Second
		}
		gcp := s.GCP
		if gcp.Proxy == "" {
			gcp.Proxy = s.Proxy
		}
		source, err := newGCPTokenSource(gcp, timeout)
		if err != nil {
			return nil, fmt.Errorf("oauth: %w", err)
		}
		return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
			token, err := source.Token(ctx)
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("oauth: %w", err)
			}
			return oauth.Auth{Token: token, Zid: s.Zid, Extensions: extensions}, nil
		}), nil
	}
	if s.Token != "" {
		return (oauth.Auth{Token: s.Token, Zid: s.Zid, Extensions: extensions}).AsMechanism(), nil
	}
	var mtls *mtlsClient
	if s.TLS.IsSet() || s.Proxy != "" {
		var err error
		if mtls, err = newMTLSClient(s.TLS, s.Proxy); err != nil {
			return nil, fmt.Errorf("oauth: tls: %w", err)
		}
	}
	certAuth := mtls != nil && mtls.CertPEM() != ""
	if s.TokenURL == "" || s.ClientID == "" || (s.ClientSecret == "" && !certAuth) {
		return nil, fmt.Errorf("oauth: token_url, client_id and client_secret (or tls client certificate) are required for client credentials")
	}
	u, err := url.Parse(s.TokenURL)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("oauth: token_url must be an absolute HTTP or HTTPS URL")
	}
	timeout := s.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	config := clientcredentials.Config{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		TokenURL:     s.TokenURL,
		Scopes:       append([]string(nil), s.Scopes...),
	}
	if s.ClientSecret == "" {
		// RFC 8705: authenticate with the certificate, send client_id in the body.
		config.AuthStyle = oauth2.AuthStyleInParams
	}
	// Serialize refreshes and cache per mechanism. Use the current authentication
	// context for each fetch, rather than retaining the first session's context.
	var mu sync.Mutex
	var token *oauth2.Token
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		mu.Lock()
		defer mu.Unlock()
		if err := ctx.Err(); err != nil {
			return oauth.Auth{}, err
		}
		if !token.Valid() {
			fetchCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			if mtls != nil {
				client, err := mtls.Client()
				if err != nil {
					return oauth.Auth{}, fmt.Errorf("oauth: tls: %w", err)
				}
				fetchCtx = context.WithValue(fetchCtx, oauth2.HTTPClient, client)
			}
			var err error
			token, err = config.Token(fetchCtx)
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("oauth: fetch token: %w", err)
			}
		}
		return oauth.Auth{Token: token.AccessToken, Zid: s.Zid, Extensions: extensions}, nil
	}), nil
}
