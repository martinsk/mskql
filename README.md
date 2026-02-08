# mskql

**A lightweight, in-memory SQL database engine written in C.**

## What is mskql?

mskql is a from-scratch SQL database that speaks the PostgreSQL wire protocol. You connect to it with any standard PostgreSQL client—`psql`, your favorite GUI, or any language driver—and run SQL against fast, in-memory tables. No configuration files. No external dependencies. Just build and go.

## Features

### Protocol
- **PostgreSQL wire protocol** — connect on port 5433 with `psql` or any Postgres-compatible client (SSL negotiation handled)

### DDL
- **Tables** — `CREATE TABLE`, `DROP TABLE`
- **Indexes** — `CREATE INDEX`, `DROP INDEX` (B-tree-style for accelerated lookups)
- **Types** — `CREATE TYPE … AS ENUM (…)`, `DROP TYPE`
- **ALTER TABLE** — `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN TYPE`

### DML
- **INSERT** — single-row, multi-row, and `INSERT … SELECT`
- **SELECT** — with `*`, column lists, aliases, and expressions
- **UPDATE** — including `UPDATE … FROM` for join-based updates
- **DELETE** — with `WHERE` filtering
- **`RETURNING`** — get back rows affected by `INSERT`, `UPDATE`, or `DELETE`
- **`ON CONFLICT DO NOTHING`** — upsert-style conflict handling

### Data Types
`INT`, `BIGINT`, `FLOAT`, `NUMERIC`/`DECIMAL`, `TEXT`, `VARCHAR(n)`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `UUID`, `SERIAL`, and user-defined `ENUM` types

### Filtering & Conditions
- `WHERE` with `AND`/`OR`/`NOT` and parenthesized grouping
- `BETWEEN`, `IN` (value lists and subqueries), `LIKE`, `ILIKE`
- `IS NULL`, `IS NOT NULL`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`
- Comparison operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`

### Joins
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`
- `NATURAL JOIN` and `JOIN … USING (col)`
- Multi-table joins

### Aggregation
- Functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `GROUP BY` (single and multi-column)
- `HAVING` for post-aggregation filtering

### Window Functions
- `ROW_NUMBER()`, `RANK()`, `SUM()`, `COUNT()`, `AVG()`
- `OVER (PARTITION BY … ORDER BY …)`

### Set Operations
- `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- `ORDER BY` on combined results

### Common Table Expressions
- `WITH name AS (SELECT …) SELECT …`

### Expressions
- Arithmetic: `+`, `-`, `*`
- `CASE WHEN … THEN … ELSE … END`
- `COALESCE(…)`

### Result Control
- `DISTINCT`
- `ORDER BY` (multi-column, individual `ASC`/`DESC` per column)
- `LIMIT` and `OFFSET`

### Constraints
- `NOT NULL`, `UNIQUE`, `PRIMARY KEY`
- `DEFAULT` values
- `CHECK` (parsed)

### Transactions
- `BEGIN`, `COMMIT`, `ROLLBACK` with snapshot-based rollback

## Getting started

### Prerequisites

A C11 compiler (e.g. `cc`, `gcc`, `clang`) and `make`. That's it.

### Build

```bash
make
```

The binary lands in `build/mskql`.

### Run

```bash
./build/mskql
```

mskql starts listening on **port 5433**. Connect with:

```bash
psql -h 127.0.0.1 -p 5433 -U test -d mskql
```

Then run SQL as usual:

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users;
```

### Run the tests

```bash
make test
```

This builds the project, starts a fresh server instance for each test case, and validates output against expected results using `psql`. The suite currently contains **160 test cases**.

## How this project was built

mskql was written entirely by AI through an **adversarial collaboration** between two AI agents:

1. **The Writer** — an AI that designed the architecture, wrote all production code, and resolved every failing test.
2. **The Challenger** — a second AI that read the source code, identified edge cases, gaps, and potential bugs, then authored targeted test cases designed to break the implementation.

The two agents worked in iterative rounds. The Challenger would study the current codebase and produce `.sql` test files exercising corner cases—empty tables, NULL handling, multi-column ordering, stale index entries after deletes, and more. The Writer would then run the new tests, diagnose failures, and ship fixes. This back-and-forth continued until the full suite of **test cases** passed cleanly.

No human wrote any of the C code or SQL test cases. The adversarial loop drove the implementation toward correctness the same way a rigorous code-review culture does on a human team—except both sides were machines.

## Project structure

```
├── src/
│   ├── main.c          # entry point, signal handling
│   ├── pgwire.c/.h     # PostgreSQL wire protocol server
│   ├── database.c/.h   # database-level operations
│   ├── table.c/.h      # table storage
│   ├── column.c/.h     # column types, enums, constraints
│   ├── row.c/.h        # row and cell representation
│   ├── parser.c/.h     # SQL parser
│   ├── query.c/.h      # query planning and execution
│   ├── index.c/.h      # index management
│   ├── stringview.h    # zero-copy string views
│   └── dynamic_array.h # generic growable array macro
├── tests/
│   ├── test.sh          # test runner
│   └── cases/           # SQL test cases
└── Makefile
```

## License

This project is provided as-is for educational and experimental purposes.
