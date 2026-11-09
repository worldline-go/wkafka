package handler

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/tlscfg"
	"github.com/worldline-go/wkafka/plugins/handler/redisauth"
)

type ConfigRedis struct {
	ClientName string   `cfg:"client_name"`
	Address    []string `cfg:"address"`
	UserName   string   `cfg:"username"`
	Password   string   `cfg:"password" log:"-"`
	// Cluster forces Redis Cluster mode, also for a single address. When false
	// and only one address is set, cluster mode is detected automatically.
	Cluster bool `cfg:"cluster"`

	TLS TLSConfig `cfg:"tls"`

	// CredentialsProvider selects a registered provider by name, for example
	// "gcp_iam" after importing plugins/handler/redisauth/gcpiam.
	CredentialsProvider string `cfg:"credentials_provider"`
	// CredentialsProviderFunc sets a provider from code. It is called for every
	// new connection and cannot be combined with username or password.
	CredentialsProviderFunc redisauth.CredentialsProvider `cfg:"-" json:"-"`
}

// TLSConfig contains options for TLS authentication.
type TLSConfig struct {
	// Enabled is whether TLS is enabled.
	Enabled bool `cfg:"enabled"`
	// InsecureSkipVerify disables server certificate and hostname verification. Unsafe for production.
	InsecureSkipVerify bool `cfg:"insecure_skip_verify"`
	// CertFile is the path to the client's TLS certificate.
	// Should be use with KeyFile.
	CertFile string `cfg:"cert_file"`
	// KeyFile is the path to the client's TLS key.
	// Should be use with CertFile.
	KeyFile string `cfg:"key_file"`
	// CAFile is the path to the CA certificate.
	// If empty, the server's root CA set will be used.
	CAFile string `cfg:"ca_file"`
}

// Generate returns a tls.Config based on the TLSConfig.
//
// If the TLSConfig is empty, nil is returned.
func (t TLSConfig) Generate() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}

	opts := []tlscfg.Opt{}

	// load client cert
	if t.CertFile != "" && t.KeyFile != "" {
		opts = append(opts, tlscfg.WithDiskKeyPair(t.CertFile, t.KeyFile))
	}

	// load CA cert
	opts = append(opts, tlscfg.WithSystemCertPool())
	if t.CAFile != "" {
		opts = append(opts, tlscfg.WithDiskCA(t.CAFile, tlscfg.ForClient))
	}

	config, err := tlscfg.New(opts...)
	if err != nil {
		return nil, err
	}
	config.InsecureSkipVerify = t.InsecureSkipVerify
	return config, nil
}

func newRedisClient(ctx context.Context, cfg ConfigRedis) (redis.UniversalClient, error) {
	if len(cfg.Address) == 0 {
		return nil, errors.New("no address provided")
	}
	for _, addr := range cfg.Address {
		if strings.TrimSpace(addr) == "" {
			return nil, errors.New("redis address must not be empty")
		}
	}

	tlsConfig, err := cfg.TLS.Generate()
	if err != nil {
		return nil, err
	}

	provider, err := cfg.credentialsProvider(ctx)
	if err != nil {
		return nil, err
	}

	client := newRedisUniversal(ctx, cfg, tlsConfig, provider)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return client, nil
}

func (cfg ConfigRedis) credentialsProvider(ctx context.Context) (redisauth.CredentialsProvider, error) {
	provider := cfg.CredentialsProviderFunc
	if cfg.CredentialsProvider != "" {
		if provider != nil {
			return nil, errors.New("credentials_provider cannot be combined with CredentialsProviderFunc")
		}

		var err error
		provider, err = redisauth.Get(ctx, cfg.CredentialsProvider)
		if err != nil {
			return nil, err
		}
	}

	if provider != nil && (cfg.UserName != "" || cfg.Password != "") {
		return nil, errors.New("credentials provider cannot be combined with username or password")
	}

	return provider, nil
}

// newRedisUniversal uses cluster mode when forced or with more than one
// address; with a single address it asks the server with INFO.
func newRedisUniversal(ctx context.Context, cfg ConfigRedis, tlsConfig *tls.Config, provider redisauth.CredentialsProvider) redis.UniversalClient {
	if cfg.Cluster || len(cfg.Address) > 1 {
		return newRedisCluster(cfg, tlsConfig, provider)
	}

	client := redis.NewClient(&redis.Options{
		Addr:       cfg.Address[0],
		Username:   cfg.UserName,
		Password:   cfg.Password,
		ClientName: cfg.ClientName,
		TLSConfig:  tlsConfig,

		CredentialsProviderContext: provider,
	})

	// INFO may be denied by an ACL; then stay standalone.
	info, err := client.Info(ctx, "cluster").Result()
	if err != nil || !clusterEnabled(info) {
		return client
	}

	_ = client.Close()

	return newRedisCluster(cfg, tlsConfig, provider)
}

func newRedisCluster(cfg ConfigRedis, tlsConfig *tls.Config, provider redisauth.CredentialsProvider) redis.UniversalClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:      cfg.Address,
		Username:   cfg.UserName,
		Password:   cfg.Password,
		ClientName: cfg.ClientName,
		TLSConfig:  tlsConfig,

		CredentialsProviderContext: provider,
	})
}

func clusterEnabled(info string) bool {
	for line := range strings.Lines(info) {
		if strings.TrimSpace(line) == "cluster_enabled:1" {
			return true
		}
	}

	return false
}
