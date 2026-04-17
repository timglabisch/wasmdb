.PHONY: all wasm frontend clean dev sync sync-types sync-install sync-dev install

all: wasm frontend

wasm:
	wasm-pack build crates/wasm-runtime --target web --out-dir ../../frontend/node_modules/wasm-runtime

frontend: wasm
	cd frontend && npm run build

dev: wasm
	cd frontend && npm run dev

clean:
	cargo clean
	rm -rf frontend/node_modules/wasm-runtime
	rm -rf frontend/dist
	rm -rf examples/sync-demo/frontend/dist
	rm -rf examples/sync-demo/frontend/wasm-pkg

sync-types:
	cargo test -p sync-demo-commands -- --test-threads=1
	mkdir -p examples/sync-demo/frontend/src/generated
	cp examples/sync-demo/commands/bindings/UserCommand.ts examples/sync-demo/frontend/src/generated/

sync: sync-types
	wasm-pack build examples/sync-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/sync-demo/frontend && npm run build && cd ../../.. && cargo run -p sync-demo-server --bin server

sync-install:
	npm install

sync-dev: sync-types
	wasm-pack build examples/sync-demo/wasm --target web --out-dir ../frontend/wasm-pkg && cd examples/sync-demo/frontend && npm run dev

install:
	npm install
