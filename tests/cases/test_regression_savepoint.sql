-- BUG: SAVEPOINT not supported (errors with 'expected SQL keyword, got SAVEPOINT')
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1);
-- input:
SAVEPOINT sp1;
-- expected output:
SAVEPOINT
-- expected status: 0
