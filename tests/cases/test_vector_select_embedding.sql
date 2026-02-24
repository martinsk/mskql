-- vector: SELECT embedding column directly through fast wire path
-- setup:
CREATE TABLE t_vembed (id INT, name TEXT, embedding VECTOR(3));
INSERT INTO t_vembed VALUES (1, 'alice', '[0.1, 0.2, 0.3]');
INSERT INTO t_vembed VALUES (2, 'bob', '[0.4, 0.5, 0.6]');
-- input:
SELECT id, embedding FROM t_vembed ORDER BY id;
-- expected output:
1|[0.1,0.2,0.3]
2|[0.4,0.5,0.6]
