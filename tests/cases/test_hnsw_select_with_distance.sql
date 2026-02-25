-- hnsw: SELECT with distance column in output
-- setup:
CREATE TABLE t_hnsw_dist (id INT, v VECTOR(3));
INSERT INTO t_hnsw_dist VALUES (1, '[1,0,0]');
INSERT INTO t_hnsw_dist VALUES (2, '[0,1,0]');
INSERT INTO t_hnsw_dist VALUES (3, '[0,0,1]');
CREATE INDEX idx_dist ON t_hnsw_dist USING hnsw (v vector_l2_ops);
-- input:
SELECT id, l2_distance(v, '[1,0,0]') FROM t_hnsw_dist ORDER BY l2_distance(v, '[1,0,0]') LIMIT 3;
-- expected output:
1|0
2|2
3|2
