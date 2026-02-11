-- window function CUME_DIST
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 100), (2, 200), (3, 200), (4, 300);
-- input:
SELECT id, score, CUME_DIST() OVER (ORDER BY score) FROM t1 ORDER BY id;
-- expected output:
1|100|0.25
2|200|0.75
3|200|0.75
4|300|1
-- expected status: 0
