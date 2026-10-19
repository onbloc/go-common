package config_test

import (
	"os"
	"path/filepath"
	"testing"

	commonconfig "github.com/onbloc/go-common/config"
)

type testConfig struct {
	App struct {
		Name  string `mapstructure:"name" default:"app"`
		Port  int    `mapstructure:"port" default:"8080"`
		Debug bool   `mapstructure:"debug" default:"false"`
	} `mapstructure:"app"`
	Database *struct {
		Password string `mapstructure:"password"`
	} `mapstructure:"database"`
}

func TestLoadPrecedence(t *testing.T) {
	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "config.yaml"), "app:\n  name: base\n  port: 8081\n")
	writeFile(t, filepath.Join(directory, "config.local.yaml"), "app:\n  name: local\n  debug: true\n")
	t.Setenv("APP_NAME", "environment")

	var target testConfig
	err := commonconfig.Load(&target, commonconfig.Options{
		Path:        filepath.Join(directory, "config.yaml"),
		Environment: "local",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if target.App.Name != "environment" || target.App.Port != 8081 || !target.App.Debug {
		t.Fatalf("Load() = %+v", target)
	}
}

func TestLoadBindsPrefixedNestedEnvironmentValue(t *testing.T) {
	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "config.yaml"), "app: {}\n")
	t.Setenv("SERVICE_DATABASE_PASSWORD", "secret")

	var target testConfig
	err := commonconfig.Load(&target, commonconfig.Options{
		Path:      filepath.Join(directory, "config.yaml"),
		EnvPrefix: "service",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if target.Database == nil || target.Database.Password != "secret" {
		t.Fatalf("Database = %+v", target.Database)
	}
}

func TestLoadIgnoresMissingOverlay(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "config.yaml"), "app:\n  name: base\n")

	var target testConfig
	if err := commonconfig.Load(&target, commonconfig.Options{
		Path:        filepath.Join(directory, "config.yaml"),
		Environment: "production",
	}); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if target.App.Name != "base" {
		t.Fatalf("App.Name = %q", target.App.Name)
	}
}

func TestLoadRejectsInvalidTarget(t *testing.T) {
	t.Parallel()

	var nilTarget *testConfig
	tests := map[string]any{
		"nil":                nil,
		"struct":             testConfig{},
		"nil struct pointer": nilTarget,
		"scalar pointer":     new(string),
	}
	for name, target := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if err := commonconfig.Load(target, commonconfig.Options{}); err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

type recursiveConfig struct {
	Name string           `mapstructure:"name"`
	Next *recursiveConfig `mapstructure:"next"`
}

func TestLoadHandlesRecursiveType(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "config.yaml"), "name: root\nnext:\n  name: child\n")

	var target recursiveConfig
	if err := commonconfig.Load(&target, commonconfig.Options{
		Path: filepath.Join(directory, "config.yaml"),
	}); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if target.Next == nil || target.Next.Name != "child" {
		t.Fatalf("Next = %+v", target.Next)
	}
}

type privateRecursiveConfig struct {
	Name string                  `mapstructure:"name"`
	next *privateRecursiveConfig `mapstructure:"next"`
}

func TestLoadIgnoresUnexportedRecursiveField(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "config.yaml"), "name: root\nnext:\n  name: child\n")

	var target privateRecursiveConfig
	if err := commonconfig.Load(&target, commonconfig.Options{
		Path: filepath.Join(directory, "config.yaml"),
	}); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if target.next != nil {
		t.Fatalf("next = %+v", target.next)
	}
}

type EmbeddedConfig struct {
	Region string `mapstructure:"region"`
}

type squashedConfig struct {
	EmbeddedConfig `mapstructure:",squash"`
}

func TestLoadSquashesEmbeddedStructExplicitly(t *testing.T) {
	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "config.yaml"), "{}\n")
	t.Setenv("REGION", "ap-northeast-2")

	var target squashedConfig
	if err := commonconfig.Load(&target, commonconfig.Options{
		Path: filepath.Join(directory, "config.yaml"),
	}); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if target.Region != "ap-northeast-2" {
		t.Fatalf("Region = %q", target.Region)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}
