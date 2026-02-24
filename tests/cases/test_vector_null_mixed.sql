-- vector: mixed NULL and non-NULL vector values
-- setup:
CREATE TABLE t_vnullm (id INT, vec VECTOR(2));
INSERT INTO t_vnullm VALUES (1, '[1.0, 2.0]');
INSERT INTO t_vnullm VALUES (2, NULL);
INSERT INTO t_vnullm VALUES (3, '[5.0, 6.0]');
-- input:
SELECT id, vec FROM t_vnullm ORDER BY id;
-- expected output:
1|[1,2]
2|
3|[5,6]
