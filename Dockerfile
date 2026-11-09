# One image for every service (controller, worker, objctl); Compose picks the
# command. See docs/DECISIONS.md DEC-023.

# ---- build stage ------------------------------------------------------------
FROM golang:1.27.3-alpine AS build
WORKDIR /src

# Download modules first so they are cached independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
# CGO_ENABLED=0: static binaries (the SQLite driver used later is pure Go).
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X objstore/internal/version.Version=${VERSION}" \
      -o /out/ ./cmd/...

# ---- runtime stage ----------------------------------------------------------
# Alpine (not distroless) so demos can `docker exec` a shell into a worker to
# inspect or deliberately corrupt chunk files, and so healthchecks can use
# busybox wget.
FROM alpine:3.24

RUN addgroup -S -g 10001 objstore \
 && adduser -S -D -H -u 10001 -G objstore objstore \
 && mkdir -p /data \
 && chown objstore:objstore /data

COPY --from=build /out/ /usr/local/bin/

# /data is the persistent volume mount point. A fresh named volume mounted
# here inherits the objstore ownership above.
ENV DATA_DIR=/data
USER objstore
WORKDIR /data

EXPOSE 9000 8080
CMD ["worker"]
