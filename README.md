# go-common

Shared Go libraries for Onbloc projects.

This repository is a multi-module monorepo. Each top-level library owns its own `go.mod`, release history, and version tags.

Module-specific usage and compatibility notes live in each module directory.

## Modules

| Module | Description | Documentation |
| --- | --- | --- |
| [`kafka`](kafka) | Sarama-based Kafka producer/consumer with batching and SASL/TLS support | [Usage](kafka/README.md) |

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for repository conventions, validation commands, and the release model.
