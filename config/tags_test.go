package config

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

const appPath = "app"

func TestMapstructureKey(t *testing.T) {
	t.Parallel()

	type Embedded struct{}
	type fields struct {
		Explicit  string `mapstructure:"custom"`
		Default   string
		Ignored   string `mapstructure:"-"`
		Embedded  `mapstructure:",squash"`
		Unwrapped Embedded
	}

	tests := []struct {
		name       string
		fieldIndex int
		wantKey    string
		wantSquash bool
		wantSkip   bool
	}{
		{name: "explicit", fieldIndex: 0, wantKey: "custom"},
		{name: "default", fieldIndex: 1, wantKey: "default"},
		{name: "ignored", fieldIndex: 2, wantSkip: true},
		{name: "squashed", fieldIndex: 3, wantSquash: true},
		{name: "named struct", fieldIndex: 4, wantKey: "unwrapped"},
	}

	targetType := reflect.TypeFor[fields]()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			key, squash, skip := mapstructureKey(targetType.Field(test.fieldIndex))
			if key != test.wantKey || squash != test.wantSquash || skip != test.wantSkip {
				t.Fatalf(
					"mapstructureKey() = (%q, %t, %t), want (%q, %t, %t)",
					key,
					squash,
					skip,
					test.wantKey,
					test.wantSquash,
					test.wantSkip,
				)
			}
		})
	}
}

func TestEnvironmentKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		path   []string
		want   string
	}{
		{name: "nested", path: []string{appPath, "log-level"}, want: "APP_LOG_LEVEL"},
		{name: "normalized prefix", prefix: "my.service", path: []string{appPath, "port"}, want: "MY_SERVICE_APP_PORT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := environmentKey(test.prefix, test.path); got != test.want {
				t.Fatalf("environmentKey() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAppendPathDoesNotMutateParent(t *testing.T) {
	t.Parallel()

	parent := make([]string, 1, 2)
	parent[0] = appPath

	child := appendPath(parent, "port")
	if !reflect.DeepEqual(parent, []string{appPath}) {
		t.Fatalf("parent = %v", parent)
	}
	if !reflect.DeepEqual(child, []string{appPath, "port"}) {
		t.Fatalf("child = %v", child)
	}
}

type databaseSettings struct {
	Host string `mapstructure:"host" default:"localhost"`
	_    string `mapstructure:"secret"`
}

type repeatedTypeConfig struct {
	Primary databaseSettings `mapstructure:"primary"`
	Replica databaseSettings `mapstructure:"replica"`
	Ignored string           `mapstructure:"-"`
}

func TestApplyTagsVisitsRepeatedSiblingTypes(t *testing.T) {
	t.Setenv("SERVICE_PRIMARY_HOST", "primary.internal")
	t.Setenv("SERVICE_REPLICA_HOST", "replica.internal")
	t.Setenv("SERVICE_PRIMARY_SECRET", "not-visible")
	t.Setenv("SERVICE_IGNORED", "not-visible")

	loader := viper.New()
	if err := applyTags(
		loader,
		reflect.TypeFor[repeatedTypeConfig](),
		nil,
		"service",
		make(map[reflect.Type]bool),
	); err != nil {
		t.Fatalf("applyTags() error = %v", err)
	}

	if got := loader.GetString("primary.host"); got != "primary.internal" {
		t.Fatalf("primary.host = %q", got)
	}
	if got := loader.GetString("replica.host"); got != "replica.internal" {
		t.Fatalf("replica.host = %q", got)
	}
	if loader.IsSet("primary.secret") {
		t.Fatal("primary.secret was registered for an unexported field")
	}
	if loader.IsSet("ignored") {
		t.Fatal("ignored field was registered")
	}
}

type directRecursiveConfig struct {
	Value string                 `mapstructure:"value" default:"root"`
	Next  *directRecursiveConfig `mapstructure:"next"`
}

type mutualRecursiveA struct {
	Value string            `mapstructure:"value" default:"root"`
	Next  *mutualRecursiveB `mapstructure:"next"`
}

type mutualRecursiveB struct {
	Previous *mutualRecursiveA `mapstructure:"previous"`
}

func TestApplyTagsStopsRecursiveTypeCycles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetType reflect.Type
	}{
		{name: "direct", targetType: reflect.TypeFor[directRecursiveConfig]()},
		{name: "mutual", targetType: reflect.TypeFor[mutualRecursiveA]()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			loader := viper.New()
			if err := applyTags(loader, test.targetType, nil, "", make(map[reflect.Type]bool)); err != nil {
				t.Fatalf("applyTags() error = %v", err)
			}
			if !loader.IsSet("value") {
				t.Fatal("root value binding was not registered")
			}
		})
	}
}
