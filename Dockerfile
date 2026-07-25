FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -mod=readonly -ldflags="-s -w" -o nats-static .

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /src/nats-static /usr/local/bin/

LABEL org.opencontainers.image.source="https://github.com/josh/nats-static"
LABEL org.opencontainers.image.description="Serve static files from a NATS object store"
LABEL org.opencontainers.image.licenses="MIT"

ENTRYPOINT ["nats-static"]
