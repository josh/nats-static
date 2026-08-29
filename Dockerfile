FROM golang:1.27-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS builder

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
