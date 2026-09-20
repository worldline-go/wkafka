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
// client credentials, or a programmatic provider.
type SaslOAuth struct {
	Enabled      bool              `cfg:"enabled" json:"enabled"`
	Zid          string            `cfg:"zid" json:"zid"`
	Extensions   map[string]string `cfg:"extensions" json:"extensions"`
	Token        string            `cfg:"token" json:"token" log:"false"`
	TokenURL     string            `cfg:"token_url" json:"token_url"`
	ClientID     string            `cfg:"client_id" json:"client_id"`
	ClientSecret string            `cfg:"client_secret" json:"client_secret" log:"false"`
	Scopes       []string          `cfg:"scopes" json:"scopes"`
	// Timeout bounds token endpoint requests; zero defaults to 10 seconds.
	Timeout time.Duration `cfg:"timeout" json:"timeout"`
	// TokenProvider is called for each authentication and owns token caching
	// and renewal. It must be safe for concurrent use. Its Auth is used as-is,
	// including Zid and Extensions, rather than merged with the config fields.
	TokenProvider func(context.Context) (oauth.Auth, error) `cfg:"-" json:"-" log:"false"`
}

func (s SaslOAuth) Generate() (sasl.Mechanism, error) {
	if !s.Enabled {
		return nil, nil
	}
	credentials := s.TokenURL != "" || s.ClientID != "" || s.ClientSecret != "" || len(s.Scopes) > 0
	modes := 0
	for _, set := range []bool{s.Token != "", s.TokenProvider != nil, credentials} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return nil, fmt.Errorf("oauth: configure exactly one of token, client credentials, or token provider")
	}
	if s.Timeout < 0 {
		return nil, fmt.Errorf("oauth: timeout must not be negative")
	}
	if s.TokenProvider != nil {
		return oauth.Oauth(s.TokenProvider), nil
	}
	extensions := maps.Clone(s.Extensions)
	if s.Token != "" {
		return (oauth.Auth{Token: s.Token, Zid: s.Zid, Extensions: extensions}).AsMechanism(), nil
	}
	if s.TokenURL == "" || s.ClientID == "" || s.ClientSecret == "" {
		return nil, fmt.Errorf("oauth: token_url, client_id and client_secret are required for client credentials")
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
			var err error
			token, err = config.Token(fetchCtx)
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("oauth: fetch token: %w", err)
			}
		}
		return oauth.Auth{Token: token.AccessToken, Zid: s.Zid, Extensions: extensions}, nil
	}), nil
}
