.PHONY: test deps

all: fmt deps build

deps:
	@go list github.com/mjibson/esc || go get github.com/mjibson/esc/...
	go generate -x
	go get ./...

clean:
	-rm -rf bin

fmt:
	gofmt -w .
	go vet .

test:
	go test ./...

bench:
	go test -bench=. ./...

sinetest:
	-rm sine.db test.png test.svg
	@ruby -e "4.times{|j| 500.times{|i| puts 'put mobius.sine'+j.to_s+' '+(1492051657000+(i*3744)).to_s+' '+(j+Math.sin(0.1*i)).to_s }}" | ./bin/mobius push sine.db
	./bin/mobius query -f png -T 'Sine Test' sine.db 'mobius.sine*' > test.png
	./bin/mobius query -f svg -T 'Sine Test' sine.db 'mobius.sine*' > test.svg

build: fmt
	go build -o bin/`basename ${PWD}` cli/*.go
