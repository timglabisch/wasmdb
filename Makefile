.PHONY: all wasm frontend clean dev

all: wasm frontend

wasm:
	wasm-pack build crates/wasm-lib --target web --out-dir ../../frontend/node_modules/wasm-lib

frontend: wasm
	cd frontend && npm run build

dev: wasm
	cd frontend && npm run dev

clean:
	cargo clean
	rm -rf frontend/node_modules/wasm-lib
	rm -rf frontend/dist

install:
	cd frontend && npm install
