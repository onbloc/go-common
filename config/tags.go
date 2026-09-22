package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

var envKeyReplacer = strings.NewReplacer(".", "_", "-", "_")

func applyTags(
	loader *viper.Viper,
	targetType reflect.Type,
	path []string,
	envPrefix string,
	visiting map[reflect.Type]bool,
) error {
	for targetType.Kind() == reflect.Ptr {
		targetType = targetType.Elem()
	}
	if targetType.Kind() != reflect.Struct || visiting[targetType] {
		return nil
	}

	visiting[targetType] = true
	defer delete(visiting, targetType)

	for index := range targetType.NumField() {
		field := targetType.Field(index)
		if !field.IsExported() {
			continue
		}

		key, squash, skip := mapstructureKey(field)
		if skip {
			continue
		}

		fieldPath := path
		if !squash && key != "" {
			fieldPath = appendPath(path, key)
		}

		fieldType := field.Type
		for fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Struct {
			if err := applyTags(loader, field.Type, fieldPath, envPrefix, visiting); err != nil {
				return err
			}
			continue
		}
		if len(fieldPath) == 0 {
			continue
		}

		configKey := strings.Join(fieldPath, ".")
		if defaultValue, ok := field.Tag.Lookup("default"); ok {
			loader.SetDefault(configKey, defaultValue)
		}
		if err := loader.BindEnv(configKey, environmentKey(envPrefix, configKey)); err != nil {
			return fmt.Errorf("bind environment variable for %q: %w", configKey, err)
		}
	}

	return nil
}

func mapstructureKey(field reflect.StructField) (key string, squash, skip bool) {
	tag, tagged := field.Tag.Lookup("mapstructure")
	if tagged {
		var options string
		key, options, _ = strings.Cut(tag, ",")
		for options != "" {
			option, remaining, found := strings.Cut(options, ",")
			if option == "squash" {
				squash = true
			}
			if !found {
				break
			}
			options = remaining
		}
	}
	if key == "-" {
		return "", false, true
	}
	if key == "" && !squash {
		key = strings.ToLower(field.Name)
	}

	return key, squash, false
}

func appendPath(path []string, key string) []string {
	result := make([]string, len(path), len(path)+1)
	copy(result, path)

	return append(result, key)
}

func environmentKey(prefix, configKey string) string {
	key := strings.ToUpper(envKeyReplacer.Replace(configKey))
	if prefix == "" {
		return key
	}

	return strings.ToUpper(envKeyReplacer.Replace(prefix)) + "_" + key
}
