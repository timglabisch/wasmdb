.PHONY: clean sync sync-types sync-install sync-dev invoice invoice-types invoice-dev invoice-dev-server install kill-sync kill-invoice

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

invoice: invoice-types kill-invoice
	wasm-pack build examples/invoice-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/invoice-demo/frontend && npm run build && cd ../../.. && cargo run -p invoice-demo-server --bin server

invoice-dev: invoice-types
	wasm-pack build examples/invoice-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/invoice-demo/frontend && npm run dev

invoice-dev-server: kill-invoice
	cargo run -p invoice-demo-server --bin server

install:
	npm install
