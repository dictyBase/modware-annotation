# Multi-architecture Docker build orchestration for modware-annotation

# Variables - customize these as needed
name := "modware-annotation"
namespace := "ghcr.io/dictybase"
tag := "latest"
dockerfile := "build/package/Dockerfile.multiarch"
platforms := "linux/amd64,linux/arm64"

# Full image reference
image := namespace + "/" + name + ":" + tag

# List all available recipes
default:
    @just --list

# Create and activate the multiarch buildx builder (safe to run multiple times)
setup-buildx:
    #!/usr/bin/env sh
    if docker buildx inspect multiarch >/dev/null 2>&1; then \
        echo "Builder 'multiarch' already exists, setting as active..."; \
        docker buildx use multiarch; \
    else \
        echo "Creating builder 'multiarch'..."; \
        docker buildx create --name multiarch --use; \
    fi
    docker buildx inspect --bootstrap

# Run Go tests before building
test:
    gotestsum --format-hide-empty-pkg --format testdox --format-icons hivis

# Build multi-architecture images for all platforms
build-multiarch:
    docker buildx build --platform {{platforms}} -t {{image}} -f {{dockerfile}} .

# Build amd64 image only for quick local testing
build-amd64:
    docker buildx build --platform linux/amd64 -t {{image}} -f {{dockerfile}} --load .

# Build arm64 image only for quick local testing
build-arm64:
    docker buildx build --platform linux/arm64 -t {{image}} -f {{dockerfile}} --load .

# Tag and push multi-architecture images to GitHub Container Registry
push-ghcr:
    docker buildx build --platform {{platforms}} -t {{image}} -f {{dockerfile}} --push .
