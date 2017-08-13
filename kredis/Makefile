.PHONY: all
all: build

.PHONY: build
build:
	go build -o ../bin/kredis

.PHONY: dist-build
dist-build:
	GOOS=linux CGO_ENABLED=0 go build -o ../dist/bin/kredis
