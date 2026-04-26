# Multi-stage. Pure-Go (modernc/sqlite) means no glibc / musl dance —
# we copy a static binary into `scratch` and ship a ~15 MB image.

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o /out/pouch-anchor .

FROM scratch
# tini-style init isn't needed: pouch-anchor's main goroutine handles
# SIGTERM directly. CA certs needed for HTTPS to pouch.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/pouch-anchor /pouch-anchor

# Persist the SQLite DB across container restarts.
VOLUME ["/data"]
ENV ANCHOR_DB=/data/drops.db
ENV ANCHOR_LISTEN=:7780
EXPOSE 7780

ENTRYPOINT ["/pouch-anchor"]
