.PHONY: all
all: build

.PHONY: build
build:
	make -C kredis build
	make -C kredis-test build

.PHONY: push
push:
	make -C kredis push
	make -C kredis-test push
