# Kittiwake container image (README "Containers"): one image runs any mode.
# The mode comes from KITTIWAKE_MODES (one per container), everything else
# from contrib/docker/kittiwake.yaml and the KITTIWAKE_* environment
# (config/env.go). Containers get their own address on an IPv6-routed
# network, so every listener is on its standard port and the management
# endpoint (metrics, health) is 9090 in every mode.
#
#   make docker               # kittiwake:<release>, every mode, all ports exposed
#   make docker-imap          # kittiwake:imap, KITTIWAKE_MODES=imap, ports 143 and 9090
#   docker build --build-arg MODE=imap --build-arg PORTS="143 9090" -t kittiwake:imap .
#
# Build and runtime take librados from download.ceph.com (CEPH_RELEASE), the
# same packages the lab boxes run; go-ceph is built against those headers.
# Plain Dockerfile syntax: it builds with the classic builder as well as
# with BuildKit (the module download is its own layer, cached by go.sum).
ARG UBUNTU=24.04

FROM ubuntu:${UBUNTU} AS build
ARG GO_VERSION=1.26.8
ARG CEPH_RELEASE=umbrella
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gcc libc6-dev pkg-config \
 && curl -fsSL https://download.ceph.com/keys/release.asc -o /etc/apt/keyrings/ceph.asc \
 && . /etc/os-release \
 && echo "deb [signed-by=/etc/apt/keyrings/ceph.asc] https://download.ceph.com/debian-${CEPH_RELEASE}/ ${VERSION_CODENAME} main" > /etc/apt/sources.list.d/ceph.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends librados-dev \
 && curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH:-amd64}.tar.gz" | tar -C /usr/local -xz \
 && rm -rf /var/lib/apt/lists/*
ENV PATH=/usr/local/go/bin:/root/go/bin:$PATH CGO_ENABLED=1 GOFLAGS=-trimpath
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=26.0+docker
ARG COMMIT=unknown
RUN go build -ldflags "-X kittiwake/app.Commit=${COMMIT} -X main.Version=${VERSION}" -o /out/kittiwake ./cmd/kittiwake \
 && go build -ldflags "-X kittiwake/app.Commit=${COMMIT} -X main.Version=${VERSION}" -o /out/kittiwake-admin ./cmd/kittiwake-admin

FROM ubuntu:${UBUNTU}
ARG CEPH_RELEASE=umbrella
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && curl -fsSL https://download.ceph.com/keys/release.asc -o /etc/apt/keyrings/ceph.asc \
 && . /etc/os-release \
 && echo "deb [signed-by=/etc/apt/keyrings/ceph.asc] https://download.ceph.com/debian-${CEPH_RELEASE}/ ${VERSION_CODENAME} main" > /etc/apt/sources.list.d/ceph.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends librados2 \
 && apt-get purge -y curl && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 61000 --user-group --home-dir /var/lib/kittiwake --shell /usr/sbin/nologin kittiwake \
 && mkdir -p /var/lib/kittiwake/spool /etc/kittiwake /etc/ceph \
 && chown -R kittiwake:kittiwake /var/lib/kittiwake
COPY --from=build /out/kittiwake /out/kittiwake-admin /usr/local/bin/
COPY contrib/docker/kittiwake.yaml /etc/kittiwake/kittiwake.yaml

# MODE pins the image to one mode (the EXPOSE list below documents its
# ports); empty leaves the choice to KITTIWAKE_MODES at run time.
ARG MODE=""
ARG PORTS="24 110 143 587 2525 8080 8081 9090"
ENV KITTIWAKE_MODES=${MODE} \
    KITTIWAKE_CONFIG=/etc/kittiwake/kittiwake.yaml \
    KITTIWAKE_ADMIN_URL=http://[::1]:8081
EXPOSE ${PORTS}
VOLUME /var/lib/kittiwake
USER kittiwake
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 CMD ["kittiwake", "health"]
ENTRYPOINT ["kittiwake"]
CMD ["serve"]
