# Dovetail build helpers. Requires Go 1.26+ and librados-dev (cgo).
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -X dovetail/app.Version=$(VERSION) -X main.Version=$(VERSION)

.PHONY: build test test-race test-sql vet fmt lint clean run-dev

build:
	go build -ldflags '$(LDFLAGS)' -o bin/dovetail ./cmd/dovetail
	go build -ldflags '$(LDFLAGS)' -o bin/dovetail-admin ./cmd/dovetail-admin

test:
	go test ./...

test-race:
	go test -race ./...

# Store conformance suites against a real MariaDB (the same suites run on
# the memory backend in `make test`). Local dev container:
#   sudo docker run -d --name dovetail-mariadb -e MARIADB_ROOT_PASSWORD=root \
#     -e MARIADB_DATABASE=dovetail -e MARIADB_USER=dovetail -e MARIADB_PASSWORD=dovetail \
#     -p 127.0.0.1:3307:3306 mariadb:11.4
DOVETAIL_TEST_DSN ?= dovetail:dovetail@tcp(127.0.0.1:3307)/dovetail
test-sql:
	DOVETAIL_TEST_DSN='$(DOVETAIL_TEST_DSN)' go test -count=1 ./store/mariadb/

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
	bin/dovetail serve -c examples/dev.yaml --seed-account wido@example.org:devpassword

# Integration tests against a real Ceph cluster (see TESTENV.md).
integration:
	go test -tags integration -count=1 -v ./rados/ -run TestLibradosConformance

# End-to-end run of the binary on the lab config.
e2e: build
	scripts/e2e.sh examples/lab.yaml

# Deploy to the lab box (TESTENV.md): binary to /usr/local/bin, restart all instances.
# ssh wants a bare IPv6 address, scp wants it in brackets.
DEPLOY_USER ?= root
DEPLOY_ADDR ?= 2a14:9b80::3143
DEPLOY_SSH   = $(DEPLOY_USER)@$(DEPLOY_ADDR)
DEPLOY_SCP   = $(DEPLOY_USER)@[$(DEPLOY_ADDR)]
deploy: build
	scp bin/dovetail $(DEPLOY_SCP):/usr/local/bin/dovetail.new
	scp bin/dovetail-admin $(DEPLOY_SCP):/usr/local/bin/dovetail-admin.new
	ssh $(DEPLOY_SSH) 'mv /usr/local/bin/dovetail.new /usr/local/bin/dovetail && mv /usr/local/bin/dovetail-admin.new /usr/local/bin/dovetail-admin && chmod 755 /usr/local/bin/dovetail /usr/local/bin/dovetail-admin && dovetail -v && dovetail-admin -v && (systemctl try-restart dovetail.target 2>/dev/null || true)'

deploy-units:
	ssh $(DEPLOY_SSH) 'install -d /etc/systemd/system /etc/dovetail/instances'
	scp contrib/systemd/dovetail@.service contrib/systemd/dovetail.target $(DEPLOY_SCP):/etc/systemd/system/
	scp contrib/systemd/instances/*.env $(DEPLOY_SCP):/etc/dovetail/instances/
	scp examples/lab.yaml $(DEPLOY_SCP):/etc/dovetail/dovetail.yaml.example
	ssh $(DEPLOY_SSH) 'test -f /etc/dovetail/dovetail.yaml || cp /etc/dovetail/dovetail.yaml.example /etc/dovetail/dovetail.yaml; systemctl daemon-reload'
