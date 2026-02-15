-- regression: NOT LIKE filters correctly
-- setup:
CREATE TABLE t (name TEXT);
INSERT INTO t VALUES ('alice'),('bob'),('alex');
-- input:
SELECT name FROM t WHERE name NOT LIKE 'al%' ORDER BY name;
-- expected output:
bob
