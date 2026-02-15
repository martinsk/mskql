-- bug: SELECT table.* returns empty rows instead of all columns
-- setup:
CREATE TABLE t_ts (id INT, name TEXT, val INT);
INSERT INTO t_ts VALUES (1, 'alice', 10), (2, 'bob', 20);
-- input:
SELECT t_ts.* FROM t_ts ORDER BY id;
-- expected output:
1|alice|10
2|bob|20
