.PHONY: all

all: lint build

lint:
	go vet ./...
	golint ./...

build:
	go install
