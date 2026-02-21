-- BUG: Adding integer to uncast string literal returns garbage instead of error
-- In PostgreSQL: operator does not exist: text + integer
-- input:
SELECT '5' + 3;
-- expected output:
ERROR:  operator does not exist: text + integer
-- expected status: 0
