package wkafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	ScramSha256 = "SCRAM-SHA-256"
	ScramSha512 = "SCRAM-SHA-512"
)

type SaslConfigs []SalsConfig

func (c SaslConfigs) Generate() ([]sasl.Mechanism, error) {
	mechanisms := []sasl.Mechanism{}

	for i := range c {
		mechanism, err := c[i].Generate()
		if err != nil {
			return nil, err
		}

		if mechanism != nil {
			mechanisms = append(mechanisms, mechanism)
		}
	}

	return mechanisms, nil
}

type SalsConfig struct {
	Plain SaslPlain `cfg:"plain" json:"plain"`
	SCRAM SaslSCRAM `cfg:"scram" json:"scram"`
}

func (c SalsConfig) Generate() (sasl.Mechanism, error) {
	if c.Plain.Enabled {
		return c.Plain.Generate()
	}

	if c.SCRAM.Enabled {
		return c.SCRAM.Generate()
	}

	return nil, nil
}

// SaslPlain contains options for SASL/SCRAM authentication.
type SaslPlain struct {
	// Enabled this config.
	Enabled bool `cfg:"enabled" json:"enabled"`
	// Zid is an optional authorization ID to use in authenticating.
	Zid string `cfg:"zid" json:"zid"`
	// User is the SASL username.
	User string `cfg:"user" json:"user"`
	// Pass is the SASL password.
	Pass string `cfg:"pass" json:"pass" log:"false"`
}

func (s SaslPlain) Generate() (sasl.Mechanism, error) {
	if !s.Enabled {
		return nil, nil
	}

	auth := plain.Auth{
		User: s.User,
		Pass: s.Pass,
	}

	return auth.AsMechanism(), nil
}

type SaslSCRAM struct {
	// Enabled this config.
	Enabled bool `cfg:"enabled" json:"enabled"`
	// Algorithm valid values are "SCRAM-SHA-256" and "SCRAM-SHA-512".
	// Empty is plain SASL.
	Algorithm string `cfg:"algorithm" json:"algorithm"`
	// Zid is an optional authorization ID to use in authenticating.
	Zid string `cfg:"zid" json:"zid"`
	// Username is the SASL username.
	User string `cfg:"user" json:"user"`
	// Pass is the SASL password.
	Pass string `cfg:"pass" json:"pass" log:"false"`
	// IsToken, if true, suffixes the "tokenauth=true" extra attribute to
	// the initial authentication message.
	//
	// Set this to true if the user and pass are from a delegation token.
	IsToken bool `cfg:"is_token" json:"is_token"`
}

func (s SaslSCRAM) Generate() (sasl.Mechanism, error) {
	if !s.Enabled {
		return nil, nil
	}

	auth := scram.Auth{
		Zid:     s.Zid,
		User:    s.User,
		Pass:    s.Pass,
		IsToken: s.IsToken,
	}

	switch s.Algorithm {
	case ScramSha256:
		return auth.AsSha256Mechanism(), nil
	case ScramSha512:
		return auth.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("invalid algorithm %q", s.Algorithm)
	}
}
