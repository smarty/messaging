#!/usr/bin/make -f

fmt:
	go mod tidy && go fmt ./...

test: fmt
	go test -timeout=1s -short -race -covermode=atomic ./...

test.db: test
	go test -timeout=30s -race -covermode=atomic github.com/smarty/messaging/v3/handlers/harness/sqladapter

test.db.local:
	(docker compose -f doc/docker-compose.yml up --wait && $(MAKE) test.db --no-print-directory); docker compose -f doc/docker-compose.yml down

compile:
	go build ./...

build: test compile

.PHONY: test fmt compile build
