# Kittiwake build helpers. Requires Go 1.26+ and librados-dev (cgo).
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -X kittiwake/app.Version=$(VERSION) -X main.Version=$(VERSION)

.PHONY: build test test-race test-sql vet fmt lint clean run-dev

build:
	go build -ldflags '$(LDFLAGS)' -o bin/kittiwake ./cmd/kittiwake
	go build -ldflags '$(LDFLAGS)' -o bin/kittiwake-admin ./cmd/kittiwake-admin

test:
	go test ./...

test-race:
	go test -race ./...

# Store conformance suites against a real MariaDB (the same suites run on
# the memory backend in `make test`). Local dev container:
#   sudo docker run -d --name kittiwake-mariadb -e MARIADB_ROOT_PASSWORD=root \
#     -e MARIADB_DATABASE=kittiwake -e MARIADB_USER=kittiwake -e MARIADB_PASSWORD=kittiwake \
#     -p 127.0.0.1:3307:3306 mariadb:11.4
# KITTIWAKE_TEST_READ_DSN points the read/write split test at a real replica
# (a second container replicating from the first, port 3308 locally).
KITTIWAKE_TEST_DSN ?= kittiwake:kittiwake@tcp(127.0.0.1:3307)/kittiwake
KITTIWAKE_TEST_READ_DSN ?=
test-sql:
	KITTIWAKE_TEST_DSN='$(KITTIWAKE_TEST_DSN)' KITTIWAKE_TEST_READ_DSN='$(KITTIWAKE_TEST_READ_DSN)' go test -count=1 ./store/mariadb/

vet:
	go vet ./...

fmt:
	gofmt -l -w .

lint: vet
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

clean:
	rm -rf bin

# Run everything in one process on the volatile in-memory backend.
run-dev: build
	bin/kittiwake serve -c examples/dev.yaml --seed-account wido@example.org:devpassword

# Integration tests against a real Ceph cluster (lab: ../kittiwake-labs).
integration:
	go test -tags integration -count=1 -v ./rados/ -run TestLibradosConformance

# End-to-end run of the binary on the lab config.
e2e: build
	scripts/e2e.sh examples/lab.yaml

# Deploy to the lab box (../kittiwake-labs): binary to /usr/local/bin, restart all instances.
# ssh wants a bare IPv6 address, scp wants it in brackets.
DEPLOY_USER ?= root
DEPLOY_ADDR ?= 2a14:9b80::3143
DEPLOY_SSH   = $(DEPLOY_USER)@$(DEPLOY_ADDR)
DEPLOY_SCP   = $(DEPLOY_USER)@[$(DEPLOY_ADDR)]
deploy: build
	scp bin/kittiwake $(DEPLOY_SCP):/usr/local/bin/kittiwake.new
	scp bin/kittiwake-admin $(DEPLOY_SCP):/usr/local/bin/kittiwake-admin.new
	ssh $(DEPLOY_SSH) 'mv /usr/local/bin/kittiwake.new /usr/local/bin/kittiwake && mv /usr/local/bin/kittiwake-admin.new /usr/local/bin/kittiwake-admin && chmod 755 /usr/local/bin/kittiwake /usr/local/bin/kittiwake-admin && kittiwake -v && kittiwake-admin -v && (systemctl try-restart kittiwake.target 2>/dev/null || true)'

deploy-units:
	ssh $(DEPLOY_SSH) 'install -d /etc/systemd/system /etc/kittiwake'
	scp contrib/systemd/kittiwake@.service contrib/systemd/kittiwake.target $(DEPLOY_SCP):/etc/systemd/system/
	scp examples/lab.yaml $(DEPLOY_SCP):/etc/kittiwake/kittiwake.yaml.example
	ssh $(DEPLOY_SSH) 'test -f /etc/kittiwake/kittiwake.yaml || cp /etc/kittiwake/kittiwake.yaml.example /etc/kittiwake/kittiwake.yaml; systemctl daemon-reload'
