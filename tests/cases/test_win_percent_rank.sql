-- window function PERCENT_RANK
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 100), (2, 200), (3, 200), (4, 300);
-- input:
SELECT id, score, PERCENT_RANK() OVER (ORDER BY score) FROM t1 ORDER BY id;
-- expected output:
1|100|0
2|200|0.333333
3|200|0.333333
4|300|1
-- expected status: 0
