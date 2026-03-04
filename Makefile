# Top-level convenience Makefile for mskql

all:
	$(MAKE) -C src
	$(MAKE) -C src release
	$(MAKE) -C src mskqlcli
	$(MAKE) -C src bench
	$(MAKE) -C src bench-throughput

release:
	$(MAKE) -C src release
	$(MAKE) -C src mskqlcli

clean:
	$(MAKE) -C src clean

test: all
	./tests/test.sh

test-concurrent: all
	$(MAKE) -C tests/cases/concurrent
	./build/test_concurrent

bench: release
	$(MAKE) -C src bench

bench-throughput: release
	$(MAKE) -C src bench-throughput

perf: bench
	bash bench/perf_analysis.sh

.PHONY: all clean release test test-concurrent bench bench-throughput perf mskqlcli
