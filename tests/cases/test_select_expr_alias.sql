-- SELECT expression with AS alias used in ORDER BY
-- setup:
CREATE TABLE t1 (id INT, price INT, qty INT);
INSERT INTO t1 (id, price, qty) VALUES (1, 10, 3), (2, 5, 10), (3, 20, 1);
-- input:
SELECT id, price * qty AS total FROM t1 ORDER BY total DESC;
-- expected output:
2|50
1|30
3|20
-- expected status: 0
