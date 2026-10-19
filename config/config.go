package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

// DefaultPath is the base configuration path used when Options.Path is empty.
const DefaultPath = "config/config.yaml"

// Options selects the files and environment-variable namespace used by Load.
//
// Zero-value Options loads DefaultPath without an environment overlay or
// environment-variable prefix.
type Options struct {
	// Path is the base configuration file. An empty Path uses DefaultPath.
	Path string
	// Environment inserts a suffix before the base file extension. For
	// example, "production" selects config.production.yaml. A missing overlay
	// is ignored.
	Environment string
	// EnvPrefix is prepended to generated environment-variable names. Dots and
	// hyphens are normalized to underscores.
	EnvPrefix string
}

// Load decodes configuration into target. Target must be a non-nil pointer to
// a struct.
//
// Load applies sources in ascending precedence: struct default tags, the
// required base YAML file, an optional environment YAML file, and non-empty OS
// environment variables. Environment-variable names are derived from exported
// fields and their mapstructure paths. Load returns file and decoding errors
// with their original causes wrapped.
func Load(target any, options Options) error {
	if err := validateTarget(target); err != nil {
		return err
	}

	configPath := options.Path
	if configPath == "" {
		configPath = DefaultPath
	}

	loader := viper.New()
	if err := applyTags(loader, reflect.TypeOf(target), nil, options.EnvPrefix, make(map[reflect.Type]bool)); err != nil {
		return fmt.Errorf("prepare config bindings: %w", err)
	}

	loader.SetConfigFile(configPath)
	if err := loader.ReadInConfig(); err != nil {
		return fmt.Errorf("read base config %q: %w", configPath, err)
	}

	if overlayPath := environmentPath(configPath, options.Environment); overlayPath != "" {
		loader.SetConfigFile(overlayPath)
		if err := loader.MergeInConfig(); err != nil && !isFileNotFound(err) {
			return fmt.Errorf("merge environment config %q: %w", overlayPath, err)
		}
	}

	if err := loader.Unmarshal(target); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}

	return nil
}

func validateTarget(target any) error {
	if target == nil {
		return errors.New("config target must be a non-nil pointer to a struct")
	}

	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Ptr || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return errors.New("config target must be a non-nil pointer to a struct")
	}

	return nil
}

func environmentPath(configPath, environment string) string {
	if environment == "" {
		return ""
	}

	extension := filepath.Ext(configPath)
	if extension == "" {
		extension = ".yaml"
	}

	name := strings.TrimSuffix(filepath.Base(configPath), filepath.Ext(configPath))
	return filepath.Join(filepath.Dir(configPath), fmt.Sprintf("%s.%s%s", name, environment, extension))
}

func isFileNotFound(err error) bool {
	var notFound viper.ConfigFileNotFoundError

	return errors.As(err, &notFound) || os.IsNotExist(err)
}
