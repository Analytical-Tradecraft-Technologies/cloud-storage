package providers

import (
	"context"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	awsprovider "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providers/aws"
)

// newAWS is deliberately private: provider-specific JSON decoding belongs here,
// while the AWS module retains its typed constructor for direct users.
func newAWS(ctx context.Context, object map[string]any) (provider.StorageProvider, error) {
	cfg, err := awsConfig(object)
	if err != nil {
		return nil, err
	}
	return awsprovider.New(ctx, cfg)
}

func awsConfig(object map[string]any) (awsprovider.AWSProviderConfig, error) {
	cfg := awsprovider.AWSProviderConfig{}
	if err := knownFields(object, "region", "profile", "temp_directory"); err != nil {
		return cfg, err
	}
	for field, target := range map[string]*string{
		"region": &cfg.Region, "profile": &cfg.Profile, "temp_directory": &cfg.TempDirectory,
	} {
		if raw, exists := object[field]; exists {
			value, ok := raw.(string)
			if !ok {
				return awsprovider.AWSProviderConfig{}, invalid("AWS configuration values must be strings")
			}
			*target = value
		}
	}
	return cfg, nil
}
