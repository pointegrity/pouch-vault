GOFLAGS ?=

.PHONY: build build-vault build-cli install test tidy clean

# Both binaries by default.
build: build-vault build-cli

build-vault:
	go build $(GOFLAGS) -o ./build/pouch-vault .

build-cli:
	go build $(GOFLAGS) -o ./build/pouch ./cmd/pouch

install:
	go install $(GOFLAGS) .
	go install $(GOFLAGS) ./cmd/pouch

test:
	go test ./...

tidy:
	go mod tidy

clean:
	rm -rf ./build
