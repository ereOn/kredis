.PHONY: all
all: build

.PHONY: build
build:
	helm lint
	helm package . --destination ../../dist/charts/
