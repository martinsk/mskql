-- vector: NULL vector values
-- setup:
CREATE TABLE t_vnull (id INT, v VECTOR(2));
INSERT INTO t_vnull VALUES (1, '[1.0, 2.0]');
INSERT INTO t_vnull VALUES (2, NULL);
INSERT INTO t_vnull VALUES (3, '[3.0, 4.0]');
-- input:
SELECT id, v IS NULL AS is_null FROM t_vnull ORDER BY id;
-- expected output:
1|f
2|t
3|f
