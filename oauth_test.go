package wkafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
)

func TestOAuthClientCredentials(t *testing.T) {
	for _, expires := range []int{3600, 1} {
		t.Run(fmt.Sprint(expires), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, ok := r.BasicAuth()
				if !ok || user != "client" || pass != "secret" || r.FormValue("grant_type") != "client_credentials" || r.FormValue("scope") != "kafka read" {
					t.Error("unexpected token request")
				}
				n := calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, n, expires)
			}))
			defer server.Close()
			mechanism, err := (SaslOAuth{Enabled: true, TokenURL: server.URL, ClientID: "client", ClientSecret: "secret", Scopes: []string{"kafka", "read"}}).Generate()
			require.NoError(t, err)
			// Canceling the first session must not poison later refreshes.
			ctx, cancel := context.WithCancel(t.Context())
			_, initial, err := mechanism.Authenticate(ctx, "broker:9092")
			cancel()
			require.NoError(t, err)
			require.Contains(t, string(initial), "auth=Bearer token-1")
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					_, _, err := mechanism.Authenticate(t.Context(), "broker:9092")
					if err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			if expires == 3600 {
				require.EqualValues(t, 1, calls.Load())
			} else {
				// oauth2 refreshes within ten seconds of expiry.
				require.EqualValues(t, 9, calls.Load())
			}
		})
	}
}

func TestOAuthStaticAndProvider(t *testing.T) {
	m, err := (SaslOAuth{Enabled: true, Token: "static", Zid: "user", Extensions: map[string]string{"cluster": "one"}}).Generate()
	require.NoError(t, err)
	require.Equal(t, "OAUTHBEARER", m.Name())
	_, initial, err := m.Authenticate(t.Context(), "broker:9092")
	require.NoError(t, err)
	require.Equal(t, "n,a=user,\x01auth=Bearer static\x01cluster=one\x01\x01", string(initial))
	wantErr := errors.New("provider failed")
	m, err = (SaslOAuth{Enabled: true, TokenProvider: func(ctx context.Context) (oauth.Auth, error) {
		require.Equal(t, t.Context(), ctx)
		return oauth.Auth{}, wantErr
	}}).Generate()
	require.NoError(t, err)
	_, _, err = m.Authenticate(t.Context(), "broker:9092")
	require.ErrorIs(t, err, wantErr)
}

func TestOAuthValidation(t *testing.T) {
	for _, config := range []SaslOAuth{
		{Enabled: true},
		{Enabled: true, Token: "token", TokenURL: "https://example.com"},
		{Enabled: true, ClientID: "client"},
		{Enabled: true, TokenURL: "/relative", ClientID: "client", ClientSecret: "secret"},
		{Enabled: true, Token: "token", Timeout: -time.Second},
		{Enabled: true, Token: "token", TokenProvider: func(context.Context) (oauth.Auth, error) { return oauth.Auth{}, nil }},
	} {
		_, err := config.Generate()
		require.Error(t, err)
	}
	m, err := (SaslOAuth{}).Generate()
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestOAuthTokenTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	m, err := (SaslOAuth{Enabled: true, TokenURL: server.URL, ClientID: "client", ClientSecret: "secret", Timeout: 20 * time.Millisecond}).Generate()
	require.NoError(t, err)
	_, _, err = m.Authenticate(t.Context(), "broker:9092")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestOAuthMainAndDLQ(t *testing.T) {
	cfg := Config{
		Brokers:  []string{"localhost:9092"},
		Security: SecurityConfig{SASL: SaslConfigs{{OAuth: SaslOAuth{Enabled: true, Token: "shared-token"}}}},
	}
	opts := options{KGOOptionsDLQ: []kgo.Opt{kgo.ClientID("custom-dlq")}}
	for _, isDLQ := range []bool{false, true} {
		client, err := newClient(&Client{}, cfg, &opts, isDLQ)
		require.NoError(t, err)
		defer client.Close()
		mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
		require.Len(t, mechanisms, 1)
		_, initial, err := mechanisms[0].Authenticate(t.Context(), "broker:9092")
		require.NoError(t, err)
		require.Contains(t, string(initial), "auth=Bearer shared-token")
	}
}
