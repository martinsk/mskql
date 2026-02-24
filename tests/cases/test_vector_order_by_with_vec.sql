-- vector: ORDER BY on non-vector column, selecting vector column
-- setup:
CREATE TABLE t_vord (id INT, score FLOAT, vec VECTOR(2));
INSERT INTO t_vord VALUES (1, 9.5, '[1.0, 2.0]');
INSERT INTO t_vord VALUES (2, 3.2, '[3.0, 4.0]');
INSERT INTO t_vord VALUES (3, 7.1, '[5.0, 6.0]');
-- input:
SELECT id, vec FROM t_vord ORDER BY score;
-- expected output:
2|[3,4]
3|[5,6]
1|[1,2]
