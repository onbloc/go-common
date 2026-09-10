# config

`config` loads application configuration from a required base YAML file, an optional environment-specific YAML overlay, and OS environment variables.

## Installation

While this repository is private, configure Go and GitHub authentication before downloading the module:

```bash
go env -w GOPRIVATE=github.com/onbloc/*
go get github.com/onbloc/go-common/config@v0.1.0
```

## Usage

```go
package bootstrap

import (
    "fmt"
    "os"

    commonconfig "github.com/onbloc/go-common/config"
)

type Config struct {
    App AppConfig `mapstructure:"app"`
}

type AppConfig struct {
    Name     string `mapstructure:"name" default:"app"`
    Port     int    `mapstructure:"port" default:"8080"`
    LogLevel string `mapstructure:"log_level" default:"info"`
}

func LoadConfig() (*Config, error) {
    target := &Config{}
    if err := commonconfig.Load(target, commonconfig.Options{
        Path:        os.Getenv("CONFIG_PATH"),
        Environment: os.Getenv("ENV"),
    }); err != nil {
        return nil, fmt.Errorf("load service config: %w", err)
    }

    return target, nil
}
```

Base configuration:

```yaml
# config/config.yaml
app:
  name: api
  port: 8080
```

Environment overlay:

```yaml
# config/config.production.yaml
app:
  log_level: warn
```

Environment override:

```bash
ENV=production APP_PORT=9090 ./app
```

## Precedence

Sources are applied from lowest to highest priority:

```text
default tag < base YAML < environment YAML < OS environment variable
```

An empty environment variable is ignored, matching Viper's default behavior.

## File selection

- `Options.Path` defaults to `config/config.yaml`.
- The base file is required.
- `Options.Environment: "production"` selects `config.production.yaml` beside the base file.
- A missing environment overlay is ignored.
- Other file read and parse failures are returned.
- Environment-name validation and mandatory-overlay policies belong to the consuming application.

## Environment variable names

Environment bindings are derived from exported struct fields and their `mapstructure` paths:

```text
app.port                     -> APP_PORT
kafka.security.sasl_password -> KAFKA_SECURITY_SASL_PASSWORD
```

Dots and hyphens become underscores. `Options.EnvPrefix` adds a normalized prefix:

```go
commonconfig.Load(&target, commonconfig.Options{
    EnvPrefix: "GNOSWAP",
})
```

```text
app.port -> GNOSWAP_APP_PORT
```

Use `mapstructure:",squash"` to flatten an embedded struct intentionally. Unexported and `mapstructure:"-"` fields are ignored.

Recursive struct types are safe to load from YAML. Automatic environment binding stops when a type recurs because an unbounded recursive environment path cannot be represented.
