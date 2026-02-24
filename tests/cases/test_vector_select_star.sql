-- vector: SELECT * includes vector column
-- setup:
CREATE TABLE t_vstar (id INT, v VECTOR(3));
INSERT INTO t_vstar VALUES (1, '[1.0, 2.0, 3.0]');
-- input:
SELECT * FROM t_vstar;
-- expected output:
1|[1,2,3]
