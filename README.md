# modware-annotation

[![License](https://img.shields.io/badge/License-BSD%202--Clause-blue.svg)](LICENSE)  
![GitHub action](https://github.com/dictyBase/modware-annotation/workflows/Continuous%20integration/badge.svg)
[![codecov](https://codecov.io/gh/dictyBase/modware-annotation/branch/develop/graph/badge.svg)](https://codecov.io/gh/dictyBase/modware-annotation)
![Last commit](https://badgen.net/github/last-commit/dictyBase/modware-annotation/develop)
[![Funding](https://badgen.net/badge/Funding/Rex%20L%20Chisholm,dictyBase,DCR/yellow?list=|)](https://projectreporter.nih.gov/project_info_description.cfm?aid=10024726&icde=0)

A gRPC-based microservice for managing annotations of biological entities in dictyBase.

## Overview

`modware-annotation` provides a comprehensive API for managing annotations of
biological features, supporting both standard and feature-specific annotations.
The service is built with a focus on performance, reliability, and
interoperability with other dictyBase services.

## Features

- Full CRUD operations for annotations
- Feature annotation with customizable properties and tags
- Publication linking (Pubmed IDs and DOIs)
- Versioned annotations with history tracking
- Ontology term integration
- Real-time event publishing via NATS messaging

## API

### gRPC

The service implements gRPC APIs defined in [dictyBase API definitions](https://github.com/dictyBase/dictybaseapis/blob/master/dictybase/annotation/annotation.proto).

## Installation

### Prerequisites

- Go 1.23+
- ArangoDB 3.6+
- NATS server

### From Source

```bash
# Clone the repository
git clone https://github.com/dictyBase/modware-annotation.git
cd modware-annotation

# Build the application
go build -o modware-annotation ./cmd/modware-annotation
```

## Usage

### Starting the Server

```bash
# Start the annotation server
./modware-annotation start-server \
  --arangodb-user <username> \
  --arangodb-pass <password> \
  --arangodb-database annotation \
  --arangodb-host <host> \
  --nats-host <nats-host> \
  --nats-port <nats-port> \
  --port 9560

# Start the feature annotation server
./modware-annotation start-feature-server \
  --arangodb-user <username> \
  --arangodb-pass <password> \
  --arangodb-database annofeature \
  --arangodb-host <host> \
  --nats-host <nats-host> \
  --nats-port <nats-port> \
  --port 9570

# Load ontologies
./modware-annotation load-ontologies \
  --arangodb-user <username> \
  --arangodb-pass <password> \
  --arangodb-database annotation \
  --arangodb-host <host> \
  --obograph <path-to-obograph.json>
```

### Configuration Options

The service can be configured using command-line flags or environment variables:

#### Common Options
- `--log-format`: Format of logging output (json|text) [default: "json"]
- `--log-level`: Log level (debug|warn|error|fatal|panic) [default: "error"]

#### Server Options
- `--port`: TCP port for the server [default: "9560" for main server, "9570" for feature server]
- `--arangodb-database, --db`: ArangoDB database name [env: ARANGODB_DATABASE]
- `--arangodb-user`: ArangoDB username
- `--arangodb-pass`: ArangoDB password
- `--arangodb-host`: ArangoDB host
- `--arangodb-port`: ArangoDB port
- `--is-secure`: Enable TLS for ArangoDB connection

#### NATS Options
- `--nats-host`: NATS server host
- `--nats-port`: NATS server port

See the command help for additional options:
```bash
./modware-annotation start-server --help
./modware-annotation start-feature-server --help
```

## Project Structure

The codebase follows a clean architecture approach with the following structure:

```
├── cmd/                      # Application entry points
│   └── modware-annotation/   # Main application
├── internal/                 # Private application code
│   ├── app/                  # Application components
│   │   ├── server/           # gRPC server implementations
│   │   ├── service/          # Service layer - business logic
│   │   └── validate/         # Input validation
│   ├── collection/           # Collection utilities
│   ├── message/              # Messaging interfaces and implementations
│   │   └── nats/             # NATS implementation
│   ├── model/                # Data models
│   └── repository/           # Data access interfaces
│       └── arangodb/         # ArangoDB implementation
```

### Key Components

- **cmd/modware-annotation**: Entry point for the CLI application
- **internal/app/server**: gRPC server implementations
- **internal/app/service**: Service layer containing business logic
- **internal/repository**: Data access layer with ArangoDB implementation
- **internal/message**: Event publishing via NATS

## Development

### Requirements

- Go 1.23+
- [gotestsum](https://github.com/gotestyourself/gotestsum) for running tests
- [gofumpt](https://github.com/mvdan/gofumpt) for code formatting
- [golangci-lint](https://golangci-lint.run/) for linting

### Running Tests

```bash
# Run all tests with detailed output
gotestsum --format-hide-empty-pkg --format testdox --format-icons hivis

# Run specific test
gotestsum --format-hide-empty-pkg --format testdox --format-icons hivis -- -run TestFindSimilar ./...

# Run tests with verbose output
gotestum --format-hide-empty-pkg --format standard-verbose --format-icons hivis
```

### Code Formatting and Linting

```bash
# Format code
gofumpt -w .

# Lint code
golangcli-lint run
```

### Go Conventions

The project adheres to the following conventions:

- **Imports**: Standard library first, then external packages, then internal packages
- **Variable Names**: camelCase, descriptive, generally at least three characters
- **Exported Names**: PascalCase for exported types, functions, and constants
- **Error Handling**: Always check errors and return meaningful wrapped errors
- **Documentation**: All exported functions, types, and constants have proper Go doc comments

For more details on coding standards, refer to [go_conventions.md](go_conventions.md).

## Docker Build

Multi-architecture images are built using `docker buildx` for `linux/amd64`
and `linux/arm64` targets. The `justfile` contains recipes for building and
pushing images.

### One-Time Developer Setup

Before using the build recipes, create the named buildx builder on your
machine. This step is required once per developer machine:

```bash
docker buildx create --name multiarch --use
docker buildx inspect --bootstrap
```

The `--use` flag makes `multiarch` the active builder for subsequent `docker
buildx` commands. The `--bootstrap` flag starts the builder daemon and
confirms it is healthy.

Alternatively, use the justfile recipe:

```bash
just setup-buildx
```

### Verify Setup

Confirm both target platforms are available:

```bash
docker buildx inspect | grep Platforms
# Expected output includes: linux/arm64, linux/amd64
```

Confirm the `multiarch` builder is active (marked with `*`):

```bash
docker buildx ls
# NAME/NODE   DRIVER/ENDPOINT  STATUS
# multiarch*  docker-container running ...
```

### Build Recipes

```bash
# Build for both platforms (no local load — intended for push)
just build-multiarch

# Build amd64 image and load into local Docker daemon
just build-amd64

# Build arm64 image and load into local Docker daemon
just build-arm64

# Build and push multi-arch manifest to GHCR
just push-ghcr
```

### Switching Builders

To switch the active builder back to Docker's default:

```bash
docker buildx use default
```

To switch back to the multiarch builder:

```bash
docker buildx use multiarch
```

## Deployment

The service is designed to be deployed as a Docker container in a Kubernetes
cluster. The deployment process is automated using GitHub Actions and Dagger.

### Docker

The application can be containerized using the included Dagger pipeline:

```bash
dagger run --with
'github.com/dictybase-docker/dagger-of-dcr/golang@11e29cedf9b2fc92547021b5043d77b28eb65921'
build
```

### Kubernetes

For Kubernetes deployment, use the Helm charts available in the [dictybase/kubernetes-charts](https://github.com/dictyBase/kubernetes-charts) repository.

## Contributing

Please read the [contributing guidelines](https://github.com/dictyBase/modware-annotation/blob/master/CONTRIBUTING.md) before submitting pull requests.

## License

[BSD-2-Clause](LICENSE)

