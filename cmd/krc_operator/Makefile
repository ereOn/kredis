.PHONY: all

all: bin/krc_operator

bin/krc_operator: *.go
	go install
	go build -o $@ $<
