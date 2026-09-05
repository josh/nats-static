FROM golang:1.27-alpine@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS builder

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
