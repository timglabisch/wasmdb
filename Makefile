.PHONY: all wasm frontend clean dev sync

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

sync:
	wasm-pack build crates/example-sync-wasm --target web --out-dir ../../crates/example-sync/frontend/wasm-pkg && cd crates/example-sync/frontend && npm run build && cd ../../.. && cargo run -p example-sync --bin server

sync-install:
	cd crates/example-sync/frontend && npm install

sync-dev:
	wasm-pack build crates/example-sync-wasm --target web --out-dir ../../crates/example-sync/frontend/wasm-pkg && cd crates/example-sync/frontend && npm run dev

install:
	cd frontend && npm install
