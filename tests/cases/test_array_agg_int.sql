-- ARRAY_AGG with integer values
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10);
INSERT INTO t1 (id, val) VALUES (2, 20);
INSERT INTO t1 (id, val) VALUES (3, 30);
-- input:
SELECT ARRAY_AGG(val) FROM t1;
-- expected output:
{10,20,30}
-- expected status: 0
