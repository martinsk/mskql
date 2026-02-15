-- regression: LATERAL JOIN with aggregate
-- setup:
CREATE TABLE t_a (id INT, name TEXT);
CREATE TABLE t_b (ref_id INT, val INT);
INSERT INTO t_a VALUES (1,'a'),(2,'b');
INSERT INTO t_b VALUES (1,100),(1,200),(2,300);
-- input:
SELECT t_a.name, lat.s FROM t_a, LATERAL (SELECT SUM(val) as s FROM t_b WHERE ref_id = t_a.id) lat ORDER BY name;
-- expected output:
a|300
b|300
