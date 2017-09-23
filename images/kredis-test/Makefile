NAME:=ereon/kredis-test

.PHONY: all
all: build

bin/kredis-test: ../../dist/bin/kredis-test
	mkdir -p bin
	cp $^ $@

.PHONY: build
build: bin/kredis-test
	docker build -t $(NAME) .

.PHONY: push
push:
	docker push $(NAME):latest
