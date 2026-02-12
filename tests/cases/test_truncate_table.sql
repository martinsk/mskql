-- TRUNCATE TABLE
-- setup:
CREATE TABLE t1 (id SERIAL, name TEXT);
INSERT INTO t1 (name) VALUES ('Alice'), ('Bob'), ('Charlie');
TRUNCATE TABLE t1;
-- input:
SELECT COUNT(*) FROM t1;
-- expected output:
0
-- expected status: 0
