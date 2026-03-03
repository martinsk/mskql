# mskql

An in-memory SQL engine optimized for analytical workloads. It speaks the PostgreSQL wire protocol — connect with `psql`, any Postgres-compatible driver, or [try it in the browser](https://martinsk.github.io/mskql/playground.html).

For documentation, benchmarks, supported SQL, and interactive tutorials, see the **[project website](https://martinsk.github.io/mskql)**.

## Quick start

Requires a C11 compiler and `make`.

```bash
make                  # build → build/mskql
./build/mskql         # starts on port 5433
psql -h 127.0.0.1 -p 5433 -U test -d mskql
```

```bash
make test             # run the test suite
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full coding guidelines.

## License

MIT License. See [LICENSE](LICENSE).
