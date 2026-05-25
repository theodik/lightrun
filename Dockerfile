# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/lightrun ./

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget

COPY --from=builder /out/lightrun /usr/local/bin/lightrun

# /health is served by the MCP listener on LIGHTRUN_MCP_PORT (default 18082).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- "http://127.0.0.1:${LIGHTRUN_MCP_PORT:-18082}/health" >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/lightrun"]
