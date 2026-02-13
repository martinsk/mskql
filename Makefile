# Top-level convenience Makefile for mskql

all:
	$(MAKE) -C src

release:
	$(MAKE) -C src release

clean:
	$(MAKE) -C src clean

test: all
	./tests/test.sh

test-concurrent: all
	$(MAKE) -C tests/cases/concurrent
	./build/test_concurrent

bench: release
	$(MAKE) -C src bench

.PHONY: all clean release test test-concurrent bench
