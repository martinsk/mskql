-- DROP TABLE IF EXISTS then recreate and query
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1);
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (42);
-- input:
SELECT id FROM t1;
-- expected output:
42
-- expected status: 0
