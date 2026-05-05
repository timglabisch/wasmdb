.PHONY: clean invoice invoice-types invoice-dev invoice-dev-server invoice-db invoice-db-down install kill-invoice render-test render-test-types render-test-dev render-test-dev-server render-test-test kill-render-test

INVOICE_COMPOSE := examples/invoice-demo/docker-compose.yml
INVOICE_SCHEMA  := examples/invoice-demo/server/sql/001_init.sql

kill-invoice:
	@lsof -ti:3124 | xargs kill -9 2>/dev/null || true

kill-render-test:
	@lsof -ti:3125 | xargs kill -9 2>/dev/null || true

clean:
	cargo clean
	rm -rf examples/invoice-demo/frontend/apps/ui/dist
	rm -rf examples/invoice-demo/frontend/apps/wasm/pkg
	rm -rf examples/render-test/frontend/apps/ui/dist
	rm -rf examples/render-test/frontend/apps/wasm/pkg

invoice-types:
	mkdir -p examples/invoice-demo/frontend/packages/generated/src
	rm -f examples/invoice-demo/frontend/packages/generated/src/*.ts
	cargo test -p invoice-demo-domain -- --test-threads=1
	# requirements.ts wird von der build.rs des wasm-Crates emittiert.
	# Nach dem Wipe oben fehlt sie — touch zwingt cargo, das Build-Script beim
	# nächsten wasm-pack-Lauf erneut auszuführen und die Datei neu zu schreiben.
	touch examples/invoice-demo/frontend/apps/wasm/build.rs

invoice: invoice-types kill-invoice
	wasm-pack build examples/invoice-demo/frontend/apps/wasm --target web --out-dir pkg && cd examples/invoice-demo/frontend/apps/ui && npm run build && cd ../../../../.. && cargo run -p invoice-demo-server --bin server

invoice-dev: invoice-types
	wasm-pack build examples/invoice-demo/frontend/apps/wasm --target web --out-dir pkg && cd examples/invoice-demo/frontend/apps/ui && npm run dev

invoice-dev-server: kill-invoice invoice-db
	cargo run -p invoice-demo-server --bin server

# Bring up a fresh TiDB for invoice-demo. Idempotent by wiping everything:
# `down -v` removes the volumes, so every invocation is a clean reset.
invoice-db:
	docker compose -f $(INVOICE_COMPOSE) down -v
	docker compose -f $(INVOICE_COMPOSE) up -d
	@echo "waiting for TiDB on :4000 ..."
	@for i in $$(seq 1 60); do \
		if docker run --rm mysql:8 mysqladmin ping -h host.docker.internal -P 4000 -u root --silent 2>/dev/null; then \
			echo "TiDB ready"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "TiDB did not come up within 120s"; \
	exit 1
	docker run --rm -i mysql:8 mysql -h host.docker.internal -P 4000 -u root < $(INVOICE_SCHEMA)
	@echo "schema applied — TiDB reset complete"

invoice-db-down:
	docker compose -f $(INVOICE_COMPOSE) down -v

install:
	npm install

# render-test: reactivity integration-test example. Echo-server (no DB), Playwright drives.
render-test-types:
	mkdir -p examples/render-test/frontend/packages/generated/src
	rm -f examples/render-test/frontend/packages/generated/src/*.ts
	cargo test -p render-test-domain -- --test-threads=1
	touch examples/render-test/frontend/apps/wasm/build.rs

render-test: render-test-types kill-render-test
	wasm-pack build examples/render-test/frontend/apps/wasm --target web --out-dir pkg && cd examples/render-test/frontend/apps/ui && npm run build && cd ../../../../.. && cargo run -p render-test-server --bin server

render-test-dev: render-test-types
	wasm-pack build examples/render-test/frontend/apps/wasm --target web --out-dir pkg && cd examples/render-test/frontend/apps/ui && npm run dev

render-test-dev-server: kill-render-test
	cargo run -p render-test-server --bin server

render-test-test: render-test-types kill-render-test
	wasm-pack build examples/render-test/frontend/apps/wasm --target web --out-dir pkg
	cd examples/render-test/frontend/apps/ui && npm run build
	cd examples/render-test/tests && npx playwright test
