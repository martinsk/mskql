-- Bug: CHECK constraint with expression involving multiple columns fails to parse
-- CREATE TABLE t (a int, b int, CHECK(a + b > 0)) raises "expected column type"
-- In PostgreSQL, table-level CHECK constraints with multi-column expressions are valid
-- setup:
-- input:
CREATE TABLE t_chk_expr (a INT, b INT, CHECK(a + b > 0));
-- expected output:
CREATE TABLE
-- expected status: 0
