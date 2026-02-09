# Top-level convenience Makefile for mskql

all:
	$(MAKE) -C src

clean:
	$(MAKE) -C src clean

test: all
	./tests/test.sh

test-concurrent: all
	$(MAKE) -C tests/cases/concurrent
	./build/test_concurrent

bench:
	$(MAKE) -C src bench

.PHONY: all clean test test-concurrent bench
