STORAGE ?= sqlite
AUTH    ?= false

ifeq ($(AUTH),true)
  PROFILE := $(STORAGE)-auth
else
  PROFILE := $(STORAGE)
endif

.PHONY: serve
serve:
	docker compose --profile $(PROFILE) up --build

.PHONY: test
test: clone
	docker compose -f test/docker-compose.yml --profile $(PROFILE) up --build --abort-on-container-exit

.PHONY: clean
clean:
	docker compose --profile all down -v
	docker compose -f test/docker-compose.yml --profile all down -v


.PHONY: clone
clone: test/resonate-test test/resonate-perf

test/resonate-test:
	@if [ -d test/resonate-test ]; then \
		echo "test/resonate-test already exists, skipping clone"; \
	else \
		git clone -b protocol-changes git@github.com:resonatehq/resonate-test.git test/resonate-test; \
	fi

test/resonate-perf:
	@if [ -d test/resonate-perf ]; then \
		echo "test/resonate-perf already exists, skipping clone"; \
	else \
		git clone git@github.com:resonatehq/resonate-perf.git test/resonate-perf; \
	fi
