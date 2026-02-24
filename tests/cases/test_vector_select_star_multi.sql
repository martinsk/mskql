-- vector: SELECT * with multiple rows and vector column
-- setup:
CREATE TABLE t_vstarm (id INT, v VECTOR(2));
INSERT INTO t_vstarm VALUES (1, '[1.5, 2.5]');
INSERT INTO t_vstarm VALUES (2, '[3.5, 4.5]');
INSERT INTO t_vstarm VALUES (3, '[5.5, 6.5]');
-- input:
SELECT * FROM t_vstarm ORDER BY id;
-- expected output:
1|[1.5,2.5]
2|[3.5,4.5]
3|[5.5,6.5]
