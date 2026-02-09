# Top-level convenience Makefile for mskql

all:
	$(MAKE) -C src

clean:
	$(MAKE) -C src clean

test: all
	./tests/test.sh

bench:
	$(MAKE) -C src bench

.PHONY: all clean test bench
