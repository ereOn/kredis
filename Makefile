.PHONY: all
all: build

.PHONY: build
build:
	make -C charts build
