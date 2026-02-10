-- UNION between INT and FLOAT columns should work (type coercion)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10);
CREATE TABLE t2 (id INT, val FLOAT);
INSERT INTO t2 (id, val) VALUES (2, 20.5);
-- input:
SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2 ORDER BY id;
-- expected output:
1|10
2|20.5
-- expected status: 0
