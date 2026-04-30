.PHONY: clean sync sync-types sync-install sync-dev invoice invoice-types invoice-dev invoice-dev-server invoice-db invoice-db-down install kill-sync kill-invoice

INVOICE_COMPOSE := examples/invoice-demo/docker-compose.yml
INVOICE_SCHEMA  := examples/invoice-demo/sql/001_init.sql

kill-sync:
	@lsof -ti:3123 | xargs kill -9 2>/dev/null || true

kill-invoice:
	@lsof -ti:3124 | xargs kill -9 2>/dev/null || true

clean:
	cargo clean
	rm -rf examples/sync-demo/frontend/dist
	rm -rf examples/sync-demo/frontend/wasm-pkg
	rm -rf examples/invoice-demo/frontend/dist
	rm -rf examples/invoice-demo/frontend/wasm-pkg

sync-types:
	cargo test -p sync-demo-commands -- --test-threads=1
	mkdir -p examples/sync-demo/frontend/src/generated
	cp examples/sync-demo/commands/bindings/UserCommand.ts examples/sync-demo/frontend/src/generated/

sync: sync-types kill-sync
	wasm-pack build examples/sync-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/sync-demo/frontend && npm run build && cd ../../.. && cargo run -p sync-demo-server --bin server

sync-install:
	npm install

sync-dev: sync-types
	wasm-pack build examples/sync-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/sync-demo/frontend && npm run dev

invoice-types:
	cargo test -p invoice-demo-commands -- --test-threads=1
	mkdir -p examples/invoice-demo/frontend/src/generated
	rm -f examples/invoice-demo/frontend/src/generated/*.ts
	for f in examples/invoice-demo/commands/bindings/*.ts; do \
	  sed 's/: bigint/: number/g; s/Array<bigint>/Array<number>/g' "$$f" \
	    > "examples/invoice-demo/frontend/src/generated/$$(basename $$f)"; \
	done
	# requirements.ts wird von der build.rs von invoice-demo-tables-client-generated emittiert.
	# Nach dem Wipe oben fehlt sie — touch zwingt cargo, das Build-Script beim
	# nächsten wasm-pack-Lauf erneut auszuführen und die Datei neu zu schreiben.
	touch examples/invoice-demo/tables-client-generated/build.rs

invoice: invoice-types kill-invoice
	wasm-pack build examples/invoice-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/invoice-demo/frontend && npm run build && cd ../../.. && cargo run -p invoice-demo-server --bin server

invoice-dev: invoice-types
	wasm-pack build examples/invoice-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/invoice-demo/frontend && npm run dev

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
