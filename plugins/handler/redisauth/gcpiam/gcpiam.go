// Package gcpiam registers the "gcp_iam" Redis credentials provider, for
// Memorystore with IAM authentication. It uses a Google Cloud access token from
// Application Default Credentials.
//
//	import _ "github.com/worldline-go/wkafka/plugins/handler/redisauth/gcpiam"
package gcpiam

import (
	"context"
	"fmt"

	"cloud.google.com/go/auth/credentials"
	"github.com/worldline-go/wkafka/plugins/handler/redisauth"
)

const (
	Name  = "gcp_iam"
	scope = "https://www.googleapis.com/auth/cloud-platform"
)

func init() {
	redisauth.Register(Name, New)
}

// New returns a provider that gives a fresh access token for every new
// connection.
func New(_ context.Context) (redisauth.CredentialsProvider, error) {
	creds, err := credentials.DetectDefault(&credentials.DetectOptions{
		Scopes: []string{scope},
	})
	if err != nil {
		return nil, fmt.Errorf("gcp_iam credentials: %w", err)
	}

	// The auth library caches and refreshes the token itself.
	return func(ctx context.Context) (string, string, error) {
		token, err := creds.Token(ctx)
		if err != nil {
			return "", "", fmt.Errorf("gcp_iam token: %w", err)
		}

		return "default", token.Value, nil
	}, nil
}
