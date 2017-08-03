.PHONY: all build update-vendor

all: build

build:
	go build -o bin/rco ./rco

update-vendor:
	dep ensure -update
