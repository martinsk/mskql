-- vector: WHERE filter on table with vector column, selecting all columns
-- setup:
CREATE TABLE t_vwhere (id INT, label TEXT, vec VECTOR(3));
INSERT INTO t_vwhere VALUES (1, 'a', '[0.1, 0.2, 0.3]');
INSERT INTO t_vwhere VALUES (2, 'b', '[0.4, 0.5, 0.6]');
INSERT INTO t_vwhere VALUES (3, 'c', '[0.7, 0.8, 0.9]');
-- input:
SELECT id, label, vec FROM t_vwhere WHERE id = 2;
-- expected output:
2|b|[0.4,0.5,0.6]
