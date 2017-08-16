.PHONY: all
all: build

bin/kredis: ../../dist/bin/kredis
	mkdir -p bin
	cp $^ $@

.PHONY: build
build: bin/kredis
	docker build -t kredis .
