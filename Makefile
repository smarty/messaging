#!/usr/bin/make -f

test: fmt
	go test -timeout=1s -short -race -covermode=atomic ./...

test.integration: test
	go test -tags=integration -v -count=1 -timeout=600s -race ./integration/

test.integration.local:
	(docker compose -f doc/docker-compose.integration.yml up --wait && $(MAKE) test.integration --no-print-directory); status=$$?; docker compose -f doc/docker-compose.integration.yml down; exit $$status

fmt:
	go mod tidy && go fmt ./...

compile:
	go build ./...

build: test compile

.PHONY: test test.integration test.integration.local fmt compile build
