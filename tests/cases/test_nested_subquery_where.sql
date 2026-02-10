-- nested subquery: WHERE val > (SELECT MAX(val) FROM t2)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 50), (3, 30);
CREATE TABLE t2 (val INT);
INSERT INTO t2 (val) VALUES (20), (25);
-- input:
SELECT id FROM t1 WHERE val > (SELECT MAX(val) FROM t2) ORDER BY id;
-- expected output:
2
3
-- expected status: 0
