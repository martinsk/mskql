# mskql

A from-scratch, in-memory SQL database written in C. It speaks the PostgreSQL wire protocol — connect with `psql`, any Postgres-compatible driver, or [try it in the browser](https://martinsk.github.io/mskql/playground.html).

This is a hobby project. For documentation, benchmarks, supported SQL, and interactive tutorials, see the **[project website](https://martinsk.github.io/mskql)**.

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

## Design principles

- **Exhaustive switch dispatch** on tagged unions — compiler-enforced via `-Wswitch-enum`, no `default:` on finite enums
- **Fail-fast** — reject unsupported inputs early, then work in a normalized world
- **Two execution paths** with identical semantics — block-oriented columnar executor (`plan.c`) with legacy row-at-a-time fallback (`query.c`)

See [CONTRIBUTING.md](CONTRIBUTING.md) for full coding guidelines.

## License

This project is provided as-is for educational and experimental purposes.
