-- DELETE all rows then COUNT(*) should return 0
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20);
-- input:
DELETE FROM t1;
SELECT COUNT(*) FROM t1;
-- expected output:
DELETE 2
0
-- expected status: 0
