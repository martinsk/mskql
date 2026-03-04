-- vector: WHERE filter preserves VECTOR data
-- setup:
CREATE TABLE t_vfilt (id INT, category INT, v VECTOR(3));
INSERT INTO t_vfilt VALUES (1, 1, '[1.0, 2.0, 3.0]');
INSERT INTO t_vfilt VALUES (2, 2, '[4.0, 5.0, 6.0]');
INSERT INTO t_vfilt VALUES (3, 1, '[7.0, 8.0, 9.0]');
INSERT INTO t_vfilt VALUES (4, 2, '[10.0, 11.0, 12.0]');
INSERT INTO t_vfilt VALUES (5, 1, '[13.0, 14.0, 15.0]');
-- input:
SELECT id, v FROM t_vfilt WHERE category = 1 ORDER BY id;
-- expected output:
1|[1,2,3]
3|[7,8,9]
5|[13,14,15]
