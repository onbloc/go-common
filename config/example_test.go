package config_test

import (
	"fmt"
	"os"
	"path/filepath"

	commonconfig "github.com/onbloc/go-common/config"
)

func ExampleLoad() {
	type appConfig struct {
		Name string `mapstructure:"name" default:"worker"`
		Port int    `mapstructure:"port" default:"8080"`
	}
	type applicationConfig struct {
		App appConfig `mapstructure:"app"`
	}

	directory, err := os.MkdirTemp("", "config-example")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(directory) }()

	path := filepath.Join(directory, "config.yaml")
	if err := os.WriteFile(path, []byte("app:\n  name: api\n"), 0o600); err != nil {
		panic(err)
	}
	if err := os.Setenv("APP_PORT", "9090"); err != nil {
		panic(err)
	}
	defer func() { _ = os.Unsetenv("APP_PORT") }()

	var target applicationConfig
	if err := commonconfig.Load(&target, commonconfig.Options{Path: path}); err != nil {
		panic(err)
	}

	fmt.Println(target.App.Name, target.App.Port)
	// Output: api 9090
}
