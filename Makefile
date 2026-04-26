BINARY ?= pouch-anchor
GOFLAGS ?=

.PHONY: build install test tidy clean

build:
	go build $(GOFLAGS) -o ./build/$(BINARY) .

install:
	go install $(GOFLAGS) .

test:
	go test ./...

tidy:
	go mod tidy

clean:
	rm -rf ./build
