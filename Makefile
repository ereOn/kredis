.PHONY: all
all: build

.PHONY: build
build:
	make -C kredis build dist-build
	make -C charts build
