-- ROW_NUMBER with DESC ordering in window
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 100), (2, 80), (3, 90);
-- input:
SELECT id, score, ROW_NUMBER() OVER (ORDER BY score DESC) FROM t1 ORDER BY id;
-- expected output:
1|100|1
2|80|3
3|90|2
-- expected status: 0
