-- RANK without PARTITION BY should rank across all rows
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 100), (2, 90), (3, 100), (4, 80);
-- input:
SELECT id, RANK() OVER (ORDER BY score DESC) FROM t1 ORDER BY id;
-- expected output:
1|1
2|3
3|1
4|4
-- expected status: 0
