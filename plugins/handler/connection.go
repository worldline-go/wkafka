package handler

import (
	"crypto/tls"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/tlscfg"
)

type ConfigRedis struct {
	ClientName string   `cfg:"client_name"`
	Address    []string `cfg:"address"`
	UserName   string   `cfg:"username"`
	Password   string   `cfg:"password"`

	TLS TLSConfig `cfg:"tls"`
}

// TLSConfig contains options for TLS authentication.
type TLSConfig struct {
	// Enabled is whether TLS is enabled.
	Enabled bool `cfg:"enabled"`
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

	return tlscfg.New(opts...)
}

func newRedisClient(cfg ConfigRedis) (redis.UniversalClient, error) {
	tlsConfig, err := cfg.TLS.Generate()
	if err != nil {
		return nil, err
	}

	if len(cfg.Address) == 0 {
		return nil, errors.New("no address provided")
	}

	if len(cfg.Address) > 1 {
		return redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:      cfg.Address,
			Username:   cfg.UserName,
			Password:   cfg.Password,
			ClientName: cfg.ClientName,
			TLSConfig:  tlsConfig,
		}), nil
	} else {
		return redis.NewClient(&redis.Options{
			Addr:       cfg.Address[0],
			Username:   cfg.UserName,
			Password:   cfg.Password,
			ClientName: cfg.ClientName,
			TLSConfig:  tlsConfig,
		}), nil
	}
}
