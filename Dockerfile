FROM golang:1.26-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS builder

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
