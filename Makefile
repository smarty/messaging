#!/usr/bin/make -f

test: fmt
	go test -timeout=1s -short -race -covermode=atomic ./...

COMPOSE := docker compose -f doc/docker-compose.integration.yml

test.integration: test
	go test -tags=integration -v -count=1 -timeout=300s -race ./integration/

# The September 2026 incident as a timeline. The Go tests only observe; the
# node stops and starts live here, where infrastructure is already managed.
# Nodes restart in the reverse order they stopped: RabbitMQ expects the last
# node down to be the first node up, and starting both at once can leave each
# waiting on the other through several 30-second retry cycles.
test.integration.ghost: export INTEGRATION_GHOST_QUEUE ?= messaging-it-ghost-$(shell date +%s)
test.integration.ghost:
	go test -tags=integration -v -count=1 -timeout=120s -race -run 'TestGhostedQueueBefore' ./integration/
	$(COMPOSE) stop rabbitmq2 && $(COMPOSE) stop rabbitmq3
	go test -tags=integration -v -count=1 -timeout=120s -race -run 'TestGhostedQueueDuring' ./integration/; status=$$?; \
		$(COMPOSE) up --wait --no-deps --no-recreate rabbitmq3 && $(COMPOSE) up --wait --no-deps --no-recreate rabbitmq2; exit $$status
	go test -tags=integration -v -count=1 -timeout=300s -race -run 'TestGhostedQueueAfter' ./integration/ \
		|| { $(COMPOSE) logs --tail=60 rabbitmq2 rabbitmq3; exit 1; }

test.integration.local:
	($(COMPOSE) up --wait && $(MAKE) test.integration test.integration.ghost --no-print-directory); status=$$?; $(COMPOSE) down; exit $$status

fmt:
	go mod tidy && go fmt ./...

compile:
	go build ./...

build: test compile

.PHONY: test test.integration test.integration.ghost test.integration.local fmt compile build
