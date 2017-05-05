.PHONY: all build glide-update

all: build

build:
	go install ./cmd/redis-cluster-operator

glide-update:
	glide update -v
