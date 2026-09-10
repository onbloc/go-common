// Package config loads typed application configuration from YAML files and OS
// environment variables.
//
// Applications define and own their configuration structs. Package config uses
// mapstructure tags to derive YAML paths and environment-variable names, and
// default tags to provide values below file and environment overrides:
//
//	type Config struct {
//		App struct {
//			Port int `mapstructure:"port" default:"8080"`
//		} `mapstructure:"app"`
//	}
//
// Load requires a base YAML file. When Options.Environment is set, it also
// merges a sibling file whose name includes that environment. For example,
// config/config.yaml with Environment "production" selects the optional
// config/config.production.yaml overlay.
//
// Environment variables have the highest precedence. Their names follow the
// full mapstructure path, converted to uppercase snake case; app.port becomes
// APP_PORT. Options.EnvPrefix can add a namespace such as SERVICE_APP_PORT.
//
// Only exported fields participate in automatic bindings. Recursive types are
// cycle-safe, and mapstructure:",squash" explicitly flattens embedded structs.
package config
