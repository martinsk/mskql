-- CEIL and FLOOR functions
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 VALUES (1, 2.3), (2, -1.7), (3, 5.0);
-- input:
SELECT id, CEIL(val), FLOOR(val) FROM t1 ORDER BY id;
-- expected output:
1|3|2
2|-1|-2
3|5|5
-- expected status: 0
