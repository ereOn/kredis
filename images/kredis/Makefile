NAME:=ereon/kredis

.PHONY: all
all: build

bin/kredis: ../../dist/bin/kredis
	mkdir -p bin
	cp $^ $@

.PHONY: build
build: bin/kredis
	docker build -t $(NAME) .

.PHONY: push
push:
	docker push $(NAME):latest
