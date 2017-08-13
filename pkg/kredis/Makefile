.PHONY: all
all: build

.PHONY: build
build:
	go test -coverprofile=coverage.out
	go tool cover -func=coverage.out
