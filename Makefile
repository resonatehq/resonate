STORAGE ?= sqlite
PROFILE := $(STORAGE)-auth

.PHONY: serve
serve:
	docker compose --profile $(PROFILE) up --build

.PHONY: clean
clean:
	docker compose --profile all down -v --remove-orphans
	docker network rm resonate 2>/dev/null || true
	docker compose -f test/docker-compose.yml --profile all down -v
