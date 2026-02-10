-- RANK() with DESC ordering should rank highest values first
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 100), (2, 200), (3, 150);
-- input:
SELECT id, RANK() OVER (ORDER BY score DESC) FROM t1;
-- expected output:
2|1
3|2
1|3
-- expected status: 0
