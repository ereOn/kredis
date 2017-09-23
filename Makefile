VERSION=0.0.1
KREDIS_SHARDS:=3
KREDIS_INSTANCES:=3

.PHONY: all
all: build dist-build

.PHONY: build
build:
	make -C pkg/kredis build
	make -C kredis build
	make -C kredis-test build

.PHONY: dist-build
dist-build:
	@mkdir -p dist/bin
	@mkdir -p dist/charts
	make -C kredis dist-build
	make -C kredis-test dist-build
	make -C images build
	make -C charts build

.PHONY: deploy
deploy:
	helm upgrade deploy dist/charts/redis-cluster-$(VERSION).tgz --install --set shards=$(KREDIS_SHARDS) --set instances=$(KREDIS_INSTANCES)

.PHONY: test
test:
	helm upgrade test dist/charts/redis-cluster-test-$(VERSION).tgz --install

.PHONY: push
push:
	make -C images push
