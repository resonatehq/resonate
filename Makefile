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


.PHONY: clone
clone: test/resonate-perf

test/resonate-perf:
	@if [ -d test/resonate-perf ]; then \
		echo "test/resonate-perf already exists, skipping clone"; \
	else \
		git clone git@github.com:resonatehq/resonate-perf.git test/resonate-perf; \
	fi
