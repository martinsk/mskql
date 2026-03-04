-- vector: ORDER BY + LIMIT preserves VECTOR data through Top-N path
-- setup:
CREATE TABLE t_vtop (id INT, score FLOAT, v VECTOR(4));
INSERT INTO t_vtop VALUES (1, 3.0, '[1.0, 2.0, 3.0, 4.0]');
INSERT INTO t_vtop VALUES (2, 1.0, '[5.0, 6.0, 7.0, 8.0]');
INSERT INTO t_vtop VALUES (3, 5.0, '[9.0, 10.0, 11.0, 12.0]');
INSERT INTO t_vtop VALUES (4, 2.0, '[13.0, 14.0, 15.0, 16.0]');
INSERT INTO t_vtop VALUES (5, 4.0, '[17.0, 18.0, 19.0, 20.0]');
-- input:
SELECT id, score, v FROM t_vtop ORDER BY score DESC LIMIT 3;
-- expected output:
3|5|[9,10,11,12]
5|4|[17,18,19,20]
1|3|[1,2,3,4]
