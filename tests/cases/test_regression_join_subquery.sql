-- regression: JOIN with subquery
-- setup:
CREATE TABLE t_a (id INT, name TEXT);
CREATE TABLE t_b (ref_id INT, val INT);
INSERT INTO t_a VALUES (1,'a'),(2,'b');
INSERT INTO t_b VALUES (1,100),(1,200),(2,300);
-- input:
SELECT t_a.name, sq.total FROM t_a JOIN (SELECT ref_id, SUM(val) as total FROM t_b GROUP BY ref_id) AS sq ON t_a.id = sq.ref_id ORDER BY name;
-- expected output:
a|300
b|300
