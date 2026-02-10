-- RANK should assign same rank to tied values and skip next
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 100), (2, 90), (3, 100), (4, 80);
-- input:
SELECT id, score, RANK() OVER (ORDER BY score DESC) FROM t1 ORDER BY id;
-- expected output:
1|100|1
2|90|3
3|100|1
4|80|4
-- expected status: 0
