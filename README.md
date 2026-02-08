# mskql

[![CodSpeed](https://img.shields.io/badge/CodSpeed-Performance%20Monitoring-blue?logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEyIDJMMiAyMkgyMkwxMiAyWiIgZmlsbD0id2hpdGUiLz4KPC9zdmc+)](https://codspeed.io/martinsk/mskql?utm_source=badge)

A lightweight SQL database implementation in C.

## Building

### Using Make

```bash
make
```

### Using CMake (for benchmarks)

```bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
make
```

## Benchmarks

This project uses [CodSpeed](https://codspeed.io) for continuous performance monitoring. Benchmarks are implemented using Google Benchmark and run automatically in CI.

To run benchmarks locally:

```bash
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCODSPEED_MODE=simulation ..
make
./mskql_benchmarks
```

## Testing

```bash
make test
```
