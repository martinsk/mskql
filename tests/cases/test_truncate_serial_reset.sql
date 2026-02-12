-- TRUNCATE TABLE resets SERIAL counters
-- setup:
CREATE TABLE t1 (id SERIAL, name TEXT);
INSERT INTO t1 (name) VALUES ('Alice'), ('Bob');
TRUNCATE t1;
INSERT INTO t1 (name) VALUES ('Charlie');
-- input:
SELECT id, name FROM t1;
-- expected output:
1|Charlie
-- expected status: 0
