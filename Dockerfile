FROM golang:1.24-bookworm AS builder

WORKDIR /app

# Install DuckDB dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy go modules first for caching
COPY go.mod go.sum ./
COPY vendor_fork/ vendor_fork/
RUN go mod download

# Copy source code
COPY . .

# Build with CGO enabled for DuckDB, using BuildKit cache for faster rebuilds
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 go build -o /minio-unfuck .

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /minio-unfuck /usr/local/bin/minio-unfuck

# Default command shows help
ENTRYPOINT ["/usr/local/bin/minio-unfuck"]
CMD ["-help"]
