-- CAST float to text
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 (id, val) VALUES (1, 3.14);
-- input:
SELECT val::TEXT FROM t1;
-- expected output:
3.14
-- expected status: 0
