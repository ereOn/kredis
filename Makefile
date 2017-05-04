.PHONY: all build glide-update

all: build

build: bin/operator

bin/operator: operator/*.go tpr/*.go
	go build -o $@ ./operator

glide-update:
	glide update -v
