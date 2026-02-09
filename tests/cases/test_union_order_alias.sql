-- union order by column alias
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (3, 'charlie'), (1, 'alice');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (4, 'dave'), (2, 'bob');
-- input:
SELECT id AS x, name FROM t1 UNION ALL SELECT id AS x, name FROM t2 ORDER BY x;
-- expected output:
1|alice
2|bob
3|charlie
4|dave
-- expected status: 0
