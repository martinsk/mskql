-- hnsw: INSERT after index creation maintains HNSW index
-- setup:
CREATE TABLE t_hnsw_ins (id INT, v VECTOR(3));
INSERT INTO t_hnsw_ins VALUES (1, '[1,0,0]');
INSERT INTO t_hnsw_ins VALUES (2, '[0,1,0]');
CREATE INDEX idx_ins ON t_hnsw_ins USING hnsw (v vector_l2_ops);
INSERT INTO t_hnsw_ins VALUES (3, '[0.9,0.1,0]');
-- input:
SELECT id FROM t_hnsw_ins ORDER BY l2_distance(v, '[1,0,0]') LIMIT 2;
-- expected output:
1
3
