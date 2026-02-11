-- DROP TABLE IF EXISTS twice should not error on second drop
-- setup:
CREATE TABLE t1 (id INT);
DROP TABLE IF EXISTS t1;
-- input:
DROP TABLE IF EXISTS t1;
-- expected output:
DROP TABLE
-- expected status: 0
