-- hnsw: nearest neighbor search with inner product distance
-- setup:
CREATE TABLE t_hnsw_ip (id INT, v VECTOR(3));
INSERT INTO t_hnsw_ip VALUES (1, '[3,0,0]');
INSERT INTO t_hnsw_ip VALUES (2, '[0,1,0]');
INSERT INTO t_hnsw_ip VALUES (3, '[0,0,1]');
INSERT INTO t_hnsw_ip VALUES (4, '[2,0,0]');
INSERT INTO t_hnsw_ip VALUES (5, '[1,0,0]');
CREATE INDEX idx_ip ON t_hnsw_ip USING hnsw (v vector_ip_ops);
-- input:
SELECT id FROM t_hnsw_ip ORDER BY inner_product(v, '[1,0,0]') LIMIT 3;
-- expected output:
1
4
5
