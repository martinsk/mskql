-- CAST int to numeric
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 100);
-- input:
SELECT val::NUMERIC FROM t1;
-- expected output:
100
-- expected status: 0
