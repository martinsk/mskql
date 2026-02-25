-- vector: l2_distance function computes squared Euclidean distance
-- setup:
CREATE TABLE t_vdist (id INT, v VECTOR(3));
INSERT INTO t_vdist VALUES (1, '[1,0,0]');
INSERT INTO t_vdist VALUES (2, '[0,1,0]');
INSERT INTO t_vdist VALUES (3, '[1,1,1]');
-- input:
SELECT id, l2_distance(v, '[0,0,0]') FROM t_vdist ORDER BY id;
-- expected output:
1|1
2|1
3|3
