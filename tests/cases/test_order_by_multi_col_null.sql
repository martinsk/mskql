-- ORDER BY multiple columns with NULLs: NULLs should sort last by default
-- setup:
CREATE TABLE t1 (id INT, a TEXT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 'x', 10), (2, NULL, 5), (3, 'x', 5), (4, NULL, 10);
-- input:
SELECT id, a, b FROM t1 ORDER BY a, b;
-- expected output:
3|x|5
1|x|10
2||5
4||10
-- expected status: 0
