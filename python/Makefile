SHELL := /bin/bash
PWD := $(shell pwd)

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow tests
.PHONY: up

local:
	cd src/tests
	echo "$(PWD)"
	MOM_HOST="localhost" PYTHONPATH="$(PWD)/src" pytest
.PHONY: local

down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

deliver:
	@echo "TEXTO DE ENTREGA:"
	@echo "Branch: rabbitmq_python"
	@(cd "$(PWD)/.."; git config --get remote.origin.url | sed -E 's#(git@github.com:|https://github.com/)([^ ]+?)(\.git)?$$#Enlace: github.com/\2#')
	@echo "Último commit:"
	@(cd "$(PWD)/.."; git rev-parse HEAD)
.PHONY: deliver