-- hnsw: nearest neighbor search with cosine distance
-- setup:
CREATE TABLE t_hnsw_cos (id INT, v VECTOR(3));
INSERT INTO t_hnsw_cos VALUES (1, '[1,0,0]');
INSERT INTO t_hnsw_cos VALUES (2, '[0,1,0]');
INSERT INTO t_hnsw_cos VALUES (3, '[0,0,1]');
INSERT INTO t_hnsw_cos VALUES (4, '[0.9,0.1,0]');
INSERT INTO t_hnsw_cos VALUES (5, '[0.5,0.5,0]');
CREATE INDEX idx_cos ON t_hnsw_cos USING hnsw (v vector_cosine_ops);
-- input:
SELECT id FROM t_hnsw_cos ORDER BY cosine_distance(v, '[1,0,0]') LIMIT 3;
-- expected output:
1
4
5
