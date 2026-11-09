package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/worldline-go/wkafka/plugins/handler/redisauth"
)

func TestTLSInsecureSkipVerify(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		config, err := (TLSConfig{Enabled: true, InsecureSkipVerify: insecure}).Generate()
		require.NoError(t, err)
		require.Equal(t, insecure, config.InsecureSkipVerify)
	}
	config, err := (TLSConfig{InsecureSkipVerify: true}).Generate()
	require.NoError(t, err)
	require.Nil(t, config)
}

func TestClusterEnabled(t *testing.T) {
	require.True(t, clusterEnabled("# Cluster\r\ncluster_enabled:1\r\n"))
	require.False(t, clusterEnabled("# Cluster\r\ncluster_enabled:0\r\n"))
	require.False(t, clusterEnabled(""))
}

func TestCredentialsProvider(t *testing.T) {
	fn := func(context.Context) (string, string, error) { return "u", "p", nil }

	_, err := ConfigRedis{CredentialsProviderFunc: fn, Password: "x"}.credentialsProvider(t.Context())
	require.Error(t, err)

	_, err = ConfigRedis{CredentialsProvider: "missing"}.credentialsProvider(t.Context())
	require.ErrorContains(t, err, "not registered")

	_, err = ConfigRedis{CredentialsProvider: "test", CredentialsProviderFunc: fn}.credentialsProvider(t.Context())
	require.Error(t, err)

	redisauth.Register("test", func(context.Context) (redisauth.CredentialsProvider, error) { return fn, nil })
	p, err := ConfigRedis{CredentialsProvider: "test"}.credentialsProvider(t.Context())
	require.NoError(t, err)
	require.NotNil(t, p)

	p, err = ConfigRedis{}.credentialsProvider(t.Context())
	require.NoError(t, err)
	require.Nil(t, p)
}
