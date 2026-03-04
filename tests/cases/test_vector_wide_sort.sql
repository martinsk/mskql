-- vector: wide VECTOR(8) through sort path preserves data
-- setup:
CREATE TABLE t_vwide (id INT, score INT, v VECTOR(8));
INSERT INTO t_vwide VALUES (1, 30, '[1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8]');
INSERT INTO t_vwide VALUES (2, 10, '[2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8]');
INSERT INTO t_vwide VALUES (3, 50, '[3.1,3.2,3.3,3.4,3.5,3.6,3.7,3.8]');
INSERT INTO t_vwide VALUES (4, 20, '[4.1,4.2,4.3,4.4,4.5,4.6,4.7,4.8]');
-- input:
SELECT id, v FROM t_vwide ORDER BY score;
-- expected output:
2|[2.1,2.2,2.3,2.4,2.5,2.6,2.7,2.8]
4|[4.1,4.2,4.3,4.4,4.5,4.6,4.7,4.8]
1|[1.1,1.2,1.3,1.4,1.5,1.6,1.7,1.8]
3|[3.1,3.2,3.3,3.4,3.5,3.6,3.7,3.8]
