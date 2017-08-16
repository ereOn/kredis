.PHONY: all
all: build dist-build

.PHONY: build
build:
	make -C pkg/kredis build
	make -C kredis build

.PHONY: dist-build
dist-build:
	make -C kredis dist-build
	make -C images build
	make -C charts build

.PHONY: push
push:
	make -C images push
