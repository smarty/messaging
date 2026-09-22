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

# A broker resource alarm as a timeline. Setting the memory watermark to zero
# raises the alarm at once; the broker blocks every publishing connection and
# stops reading from it. 0.4 is the image's default, so the last step restores
# normal service even if the during-phase test fails.
test.integration.alarm: export INTEGRATION_ALARM_QUEUE ?= messaging-it-alarm-$(shell date +%s)
test.integration.alarm:
	go test -tags=integration -v -count=1 -timeout=120s -race -run 'TestResourceAlarmBefore' ./integration/
	$(COMPOSE) exec -T rabbitmq1 rabbitmqctl set_vm_memory_high_watermark 0
	go test -tags=integration -v -count=1 -timeout=120s -race -run 'TestResourceAlarmDuring' ./integration/; status=$$?; \
		$(COMPOSE) exec -T rabbitmq1 rabbitmqctl set_vm_memory_high_watermark 0.4; exit $$status
	go test -tags=integration -v -count=1 -timeout=120s -race -run 'TestResourceAlarmAfter' ./integration/

test.integration.local:
	($(COMPOSE) up --wait && $(MAKE) test.integration test.integration.ghost test.integration.alarm --no-print-directory); status=$$?; $(COMPOSE) down; exit $$status

fmt:
	go mod tidy && go fmt ./...

compile:
	go build ./...

build: test compile

.PHONY: test test.integration test.integration.ghost test.integration.alarm test.integration.local fmt compile build
