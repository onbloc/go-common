package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

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
		if err := loader.BindEnv(configKey, environmentKey(envPrefix, fieldPath)); err != nil {
			return fmt.Errorf("bind environment variable for %q: %w", configKey, err)
		}
	}

	return nil
}

func mapstructureKey(field reflect.StructField) (key string, squash, skip bool) {
	tag, tagged := field.Tag.Lookup("mapstructure")
	parts := strings.Split(tag, ",")
	if tagged {
		key = parts[0]
	}
	if key == "-" {
		return "", false, true
	}
	for _, option := range parts[1:] {
		if option == "squash" {
			squash = true
		}
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

func environmentKey(prefix string, path []string) string {
	normalize := strings.NewReplacer(".", "_", "-", "_")
	key := strings.ToUpper(normalize.Replace(strings.Join(path, ".")))
	if prefix == "" {
		return key
	}

	return strings.ToUpper(normalize.Replace(prefix)) + "_" + key
}
