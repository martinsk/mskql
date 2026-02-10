-- SELECT with arithmetic expression alias used in ORDER BY
-- setup:
CREATE TABLE t1 (id INT, price INT, qty INT);
INSERT INTO t1 (id, price, qty) VALUES (1, 10, 5), (2, 20, 2), (3, 5, 10);
-- input:
SELECT id, price * qty AS total FROM t1 ORDER BY total;
-- expected output:
2|40
1|50
3|50
-- expected status: 0
