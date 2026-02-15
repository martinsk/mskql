-- bug: val::INT in WHERE clause fails with "expected comparison operator"
-- setup:
CREATE TABLE t_ciw (id INT, val TEXT);
INSERT INTO t_ciw VALUES (1, '10'), (2, '20'), (3, '5'), (4, '30');
-- input:
SELECT id FROM t_ciw WHERE val::INT > 15 ORDER BY id;
-- expected output:
2
4
