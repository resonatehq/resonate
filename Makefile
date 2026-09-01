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

# The web console.
#
# `assets/` is the built app, committed, so `cargo build` alone produces the
# shipping binary — no node on the build machine and nothing to install. This
# is what regenerates it after a change under `crates/resonate-gateway-web/ui`,
# and the result belongs in the same commit as the change.
CONSOLE := crates/resonate-gateway-web

.PHONY: console
console:
	cd $(CONSOLE)/ui && npm ci && npm run build
	rm -rf $(CONSOLE)/assets
	cp -r $(CONSOLE)/ui/build $(CONSOLE)/assets

# The console against a server you started yourself, with hot reload.
.PHONY: console-dev
console-dev:
	cd $(CONSOLE)/ui && npm install && npm run dev
