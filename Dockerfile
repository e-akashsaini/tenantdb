# syntax=docker/dockerfile:1

# Build on bookworm so the binary's glibc matches the distroless runtime image.
ARG GO_VERSION=1.27.2
FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY proxy/go.mod proxy/go.sum ./
RUN go mod download
COPY proxy/ ./
ARG VERSION=dev
# cgo: the SQL parser is PostgreSQL's own, compiled in.
RUN CGO_ENABLED=1 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/tenantdb ./cmd/tenantdb

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /out/tenantdb /usr/local/bin/tenantdb
# 6432: the proxy. The admin endpoints (metrics, health) stay on the container's
# loopback unless TENANTDB_ADMIN_ADDR says otherwise.
EXPOSE 6432
ENV TENANTDB_LISTEN=:6432 TENANTDB_ADMIN_ADDR=127.0.0.1:9187
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 CMD ["/usr/local/bin/tenantdb", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/tenantdb"]
