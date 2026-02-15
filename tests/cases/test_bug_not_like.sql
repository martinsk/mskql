-- bug: NOT LIKE fails with "expected IN after NOT"
-- setup:
CREATE TABLE t_nl (id INT, name TEXT);
INSERT INTO t_nl VALUES (1, 'alice'), (2, 'bob'), (3, 'alex'), (4, 'charlie');
-- input:
SELECT name FROM t_nl WHERE name NOT LIKE 'al%' ORDER BY name;
-- expected output:
bob
charlie
