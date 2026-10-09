package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
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
