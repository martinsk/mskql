-- COUNT(*) should count all rows; COUNT(col) should count non-NULL only
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 30);
-- input:
SELECT COUNT(*), COUNT(val) FROM t1;
-- expected output:
3|2
-- expected status: 0
