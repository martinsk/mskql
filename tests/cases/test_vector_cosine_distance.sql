-- vector: cosine_distance function computes 1 - cos(a,b)
-- setup:
CREATE TABLE t_vcos (id INT, v VECTOR(3));
INSERT INTO t_vcos VALUES (1, '[1,0,0]');
INSERT INTO t_vcos VALUES (2, '[0,1,0]');
INSERT INTO t_vcos VALUES (3, '[1,1,0]');
-- input:
SELECT id, cosine_distance(v, '[1,0,0]') FROM t_vcos ORDER BY id;
-- expected output:
1|0
2|1
3|0.292893
