-- vector: basic create table, insert, and select
-- setup:
CREATE TABLE t_vec (id INT, embedding VECTOR(3));
INSERT INTO t_vec VALUES (1, '[1.0, 2.0, 3.0]');
INSERT INTO t_vec VALUES (2, '[4.5, 5.5, 6.5]');
-- input:
SELECT id FROM t_vec ORDER BY id;
-- expected output:
1
2
