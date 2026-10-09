package wkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSASLConfig(t *testing.T) {
	_, err := (SaslConfigs{{Plain: SaslPlain{Enabled: true}, SCRAM: SaslSCRAM{Enabled: true}}}).Generate()
	require.ErrorContains(t, err, "sasl[0]: only one")
	_, err = (SaslConfig{Plain: SaslPlain{Enabled: true}, OAuth: SaslOAuth{Enabled: true, Token: "token"}}).Generate()
	require.Error(t, err)
	mechanisms, err := (SaslConfigs{
		{},
		{Plain: SaslPlain{Enabled: true, Zid: "identity", User: "user", Pass: "pass"}},
		{SCRAM: SaslSCRAM{Enabled: true, Algorithm: ScramSha256}},
		{SCRAM: SaslSCRAM{Enabled: true, Algorithm: ScramSha512}},
		{OAuth: SaslOAuth{Enabled: true, Token: "token"}},
	}).Generate()
	require.NoError(t, err)
	require.Len(t, mechanisms, 4)
	for i, name := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"} {
		require.Equal(t, name, mechanisms[i].Name())
	}
	_, initial, err := mechanisms[0].Authenticate(t.Context(), "broker:9092")
	require.NoError(t, err)
	require.Equal(t, "identity\x00user\x00pass", string(initial))
	_, err = (SaslSCRAM{Enabled: true}).Generate()
	require.ErrorContains(t, err, "invalid algorithm")
}

func TestTLSIncompleteKeyPair(t *testing.T) {
	for _, config := range []TLSConfig{
		{Enabled: true, CertFile: "cert.pem"},
		{Enabled: true, KeyFile: "key.pem"},
	} {
		_, err := config.Generate()
		require.ErrorContains(t, err, "provided together")
	}
	config, err := (TLSConfig{CertFile: "ignored.pem"}).Generate()
	require.NoError(t, err)
	require.Nil(t, config)
}

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
