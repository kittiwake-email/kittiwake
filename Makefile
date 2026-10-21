# Dovetail build helpers. Requires Go 1.26+ and librados-dev (cgo).
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  = -X dovetail/app.Version=$(VERSION)

.PHONY: build test test-race vet fmt lint clean run-dev

build:
	go build -ldflags '$(LDFLAGS)' -o bin/dovetail ./cmd/dovetail

test:
	go test ./...

test-race:
	go test -race ./...

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
	bin/dovetail serve --config examples/dev.yaml --seed-account wido@example.org:devpassword
