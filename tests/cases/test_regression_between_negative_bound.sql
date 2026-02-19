-- bug: BETWEEN with negative lower bound fails (parser misinterprets unary minus)
-- setup:
CREATE TABLE t_between_neg (id INT, val INT);
INSERT INTO t_between_neg VALUES (1, -5), (2, 0), (3, 5), (4, -10), (5, 10);
-- input:
SELECT * FROM t_between_neg WHERE val BETWEEN -5 AND 5 ORDER BY id;
-- expected output:
1|-5
2|0
3|5
-- expected status: 0
