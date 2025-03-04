DOCKER_COMPOSE := $(or $(shell command -v docker-compose),docker compose)

.PHONY: restart

restart:
	$(DOCKER_COMPOSE) down
	$(DOCKER_COMPOSE) build
	$(DOCKER_COMPOSE) up -d

.PHONY: fetch-aws
fetch-aws:
	echo python ./clickhouse-sync.py --remote-host $(AWS_HOST) --wipe-tables

