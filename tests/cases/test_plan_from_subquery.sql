-- Test FROM subquery through plan executor
-- setup:
CREATE TABLE t_fs (id INT, val INT);
INSERT INTO t_fs VALUES (1, 10);
INSERT INTO t_fs VALUES (2, 20);
INSERT INTO t_fs VALUES (3, 30);
-- input:
SELECT * FROM (SELECT id, val FROM t_fs WHERE val > 10) AS sub ORDER BY id;
-- expected output:
2|20
3|30
