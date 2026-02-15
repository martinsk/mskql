-- bug: boolean comparison expression in SELECT list fails with parse error
-- setup:
CREATE TABLE t_be (id INT, a INT, b INT);
INSERT INTO t_be VALUES (1, 10, 20), (2, 30, 20);
-- input:
SELECT id, (a > b) as a_gt_b FROM t_be ORDER BY id;
-- expected output:
1|f
2|t
