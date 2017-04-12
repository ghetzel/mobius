.PHONY: test deps

all: fmt deps build

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	go generate -x
	go get .

clean:
	-rm -rf bin

fmt:
	gofmt -w .
	go vet .

test:
	go test ./...

build: fmt
	go build -o bin/`basename ${PWD}` cli/*.go
