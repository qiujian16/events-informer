.PHONY: build
build:
	go build -o bin/sender cmd/sender/sender.go
	go build -o bin/syncer cmd/syncer/syncer.go