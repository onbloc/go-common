# Contributing

Thank you for contributing to `go-common`. This repository contains independently versioned Go modules shared by Onbloc projects. Changes must keep each module small, stable, and usable without repository-specific assumptions.

## Prerequisites

- Git
- Go versions declared by the affected modules
- `make`
- `golangci-lint` v2.8.0 for local linting

Install the pinned linter:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.8.0
```

## Repository layout

This repository is a multi-module monorepo. Each top-level library is an independent Go module.

```text
go-common/
├── module-a/
│   ├── go.mod
│   └── ...
└── module-b/
    ├── go.mod
    └── ...
```

Do not add a root `go.mod`. A library must keep its source, tests, dependencies, and module documentation inside its own directory.

The module path must match its directory:

```text
Directory: module-a/
Module:    github.com/onbloc/go-common/module-a
```

## Branches

Create branches from the latest `main`. Use a short lowercase description with a conventional prefix:

```text
feat/module-a
fix/module-a-validation
docs/contribution-guide
ci/module-validation
```

Keep a branch focused on one module or one repository-level concern. Do not combine unrelated cleanup with a functional change.

## Commits

Use Conventional Commits with an English, imperative subject:

```text
feat: add module-a
fix: reject invalid module-a input
docs: clarify release process
ci: validate independent modules
```

- Keep the subject concise.
- Explain motivation and compatibility impact in the body when the reason is not obvious.
- Do not include generated noise, credentials, local state, or AI attribution.
- Rebase or squash fixup commits before merge when requested by the reviewer.

## Go conventions

- Follow standard Go naming and package conventions.
- Keep exported APIs minimal; unexported implementation is preferred.
- Add Go documentation comments to exported identifiers.
- Return actionable errors and preserve causes with `%w`.
- Avoid global mutable state and package initialization side effects.
- Prefer the standard library before adding a dependency.
- Do not expose types from an implementation dependency unless that dependency is intentionally part of the public API.
- Preserve backward compatibility within a major version.

Run formatting before committing:

```bash
make fmt
```

## Dependencies

Dependencies belong to the module that uses them.

- Pin direct dependencies in the module's `go.mod`.
- Commit the corresponding `go.sum`.
- Run `go mod tidy` in every affected module.
- Do not introduce a dependency only to replace a small standard-library implementation.
- Explain security-sensitive or unusually large dependencies in the pull request.

Validate module files without modifying the working tree:

```bash
make tidy-check
```

## Tests

Tests must cover observable behavior, boundaries, and failure cases. Keep them deterministic, isolated, and safe to run in parallel unless the test explicitly documents a shared resource.

Run:

```bash
make test
make lint
```

`make check` runs all required local checks.

Bug fixes should include a regression test when a stable behavioral assertion can reproduce the defect. Avoid tests that only assert implementation details.

## Pull requests

A pull request must:

- Describe the problem and the chosen behavior.
- Identify the affected module and compatibility impact.
- Include tests for new or changed behavior.
- Update module documentation when public usage changes.
- Pass formatting, module-file, test, vet, and lint checks.
- Avoid unrelated file changes.

Before requesting review:

```bash
make check
```

## Versioning and releases

Modules follow semantic versioning independently. A subdirectory module tag includes its directory prefix:

```text
module-a/v0.1.0
module-a/v0.2.0
module-b/v1.0.0
```

Releasing one module does not change another module's version. A release currently requires only an immutable annotated Git tag on a commit merged into `main`; no artifact upload is required.

For a v2 or later incompatible release, follow Go's major-version module rule:

```text
Module path: github.com/onbloc/go-common/module-a/v2
Tag:         module-a/v2.0.0
Import:      github.com/onbloc/go-common/module-a/v2
```

Never move or overwrite a published version tag. Publish a new patch, minor, or major version instead.
