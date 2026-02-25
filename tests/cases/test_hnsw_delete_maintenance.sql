-- hnsw: DELETE removes vectors from HNSW index
-- setup:
CREATE TABLE t_hnsw_del (id INT, v VECTOR(3));
INSERT INTO t_hnsw_del VALUES (1, '[1,0,0]');
INSERT INTO t_hnsw_del VALUES (2, '[0.9,0.1,0]');
INSERT INTO t_hnsw_del VALUES (3, '[0,1,0]');
CREATE INDEX idx_del ON t_hnsw_del USING hnsw (v vector_l2_ops);
DELETE FROM t_hnsw_del WHERE id = 2;
-- input:
SELECT id FROM t_hnsw_del ORDER BY l2_distance(v, '[1,0,0]') LIMIT 2;
-- expected output:
1
3
