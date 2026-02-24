-- vector: LIMIT with vector column
-- setup:
CREATE TABLE t_vlim (id INT, vec VECTOR(3));
INSERT INTO t_vlim VALUES (1, '[0.1, 0.2, 0.3]');
INSERT INTO t_vlim VALUES (2, '[0.4, 0.5, 0.6]');
INSERT INTO t_vlim VALUES (3, '[0.7, 0.8, 0.9]');
-- input:
SELECT id, vec FROM t_vlim ORDER BY id LIMIT 2;
-- expected output:
1|[0.1,0.2,0.3]
2|[0.4,0.5,0.6]
