RC = rustc
LIB = src/map_reduce.rs

lib: $(LIB)
	$(RC) --crate-type=lib $^ --out-dir lib

test: lib examples/word-count.rs
	rustc --test  -L lib examples/word-count.rs -o bin/word-count
	./bin/word-count
