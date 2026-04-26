GOFLAGS ?=

.PHONY: build build-anchor build-cli install test tidy clean

# Both binaries by default.
build: build-anchor build-cli

build-anchor:
	go build $(GOFLAGS) -o ./build/pouch-anchor .

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
