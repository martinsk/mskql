-- DROP TABLE IF EXISTS on existing table
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1);
-- input:
DROP TABLE IF EXISTS t1;
-- expected output:
DROP TABLE
-- expected status: 0
