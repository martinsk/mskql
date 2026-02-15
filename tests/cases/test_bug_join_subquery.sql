-- bug: JOIN (subquery) AS alias fails with "expected table name after JOIN"
-- setup:
CREATE TABLE t_js1 (id INT, name TEXT);
CREATE TABLE t_js2 (ref_id INT, val INT);
INSERT INTO t_js1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t_js2 VALUES (1, 100), (1, 200), (2, 300);
-- input:
SELECT t_js1.name, sq.total FROM t_js1 JOIN (SELECT ref_id, SUM(val) as total FROM t_js2 GROUP BY ref_id) AS sq ON t_js1.id = sq.ref_id ORDER BY name;
-- expected output:
alice|300
bob|300
