# syntax=docker/dockerfile:1.7
#
# Targets:
#   standalone (default) — iag-traceability repo root on Railway
#   monorepo             — IAG_multi_backend root context (deploy/docker-compose)
#
# Monorepo:   docker build -f services/operations/traceability/Dockerfile --target monorepo .
# Standalone: docker build --target standalone .

FROM golang:1.25-alpine AS base
RUN apk add --no-cache git ca-certificates
ENV PLATFORM_GO_DEP=/deps/platform-go

FROM base AS platform-go-clone
ARG IAG_META_REF=main
ARG IAG_META_REPO=https://github.com/AlexanderKiyingi/IAG_multi_backend.git
RUN git clone --depth 1 --branch "${IAG_META_REF}" "${IAG_META_REPO}" /tmp/iag \
    && mv /tmp/iag/shared/platform-go "${PLATFORM_GO_DEP}" \
    && rm -rf /tmp/iag

FROM base AS platform-go-copy
COPY shared/platform-go ${PLATFORM_GO_DEP}

FROM base AS build-standalone
COPY --from=platform-go-clone ${PLATFORM_GO_DEP} ${PLATFORM_GO_DEP}
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod edit -replace=github.com/alvor-technologies/iag-platform-go=${PLATFORM_GO_DEP} \
    && go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /traceability ./cmd/server

FROM base AS build-monorepo
COPY --from=platform-go-copy ${PLATFORM_GO_DEP} ${PLATFORM_GO_DEP}
WORKDIR /src/services/operations/traceability
COPY services/operations/traceability/go.mod services/operations/traceability/go.sum ./
RUN go mod edit -replace=github.com/alvor-technologies/iag-platform-go=${PLATFORM_GO_DEP} \
    && go mod download
COPY services/operations/traceability/ .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /traceability ./cmd/server

FROM alpine:3.21 AS monorepo
RUN apk add --no-cache ca-certificates tzdata wget
WORKDIR /app
COPY --from=build-monorepo /traceability /app/traceability
ENV PORT=4011 \
    GIN_MODE=release \
    AUTO_MIGRATE=false
EXPOSE 4011
HEALTHCHECK --interval=15s --timeout=5s --start-period=25s --retries=5 \
  CMD wget -q -O /dev/null http://127.0.0.1:4011/ready || exit 1
USER nobody
ENTRYPOINT ["/app/traceability"]

FROM alpine:3.21 AS standalone
RUN apk add --no-cache ca-certificates tzdata wget
WORKDIR /app
COPY --from=build-standalone /traceability /app/traceability
ENV PORT=4011 \
    GIN_MODE=release \
    AUTO_MIGRATE=false
EXPOSE 4011
HEALTHCHECK --interval=15s --timeout=5s --start-period=25s --retries=5 \
  CMD wget -q -O /dev/null http://127.0.0.1:4011/ready || exit 1
USER nobody
ENTRYPOINT ["/app/traceability"]
