-- hnsw: CREATE INDEX USING hnsw on vector column
-- setup:
CREATE TABLE t_hnsw (id INT, v VECTOR(3));
INSERT INTO t_hnsw VALUES (1, '[1,0,0]');
INSERT INTO t_hnsw VALUES (2, '[0,1,0]');
INSERT INTO t_hnsw VALUES (3, '[0,0,1]');
INSERT INTO t_hnsw VALUES (4, '[1,1,0]');
INSERT INTO t_hnsw VALUES (5, '[1,1,1]');
CREATE INDEX idx_hnsw ON t_hnsw USING hnsw (v vector_l2_ops);
-- input:
SELECT id FROM t_hnsw ORDER BY l2_distance(v, '[1,0,0]') LIMIT 3;
-- expected output:
1
4
2
