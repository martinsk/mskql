-- UNION ALL with ORDER BY
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (3, 'charlie'), (1, 'alice');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (2, 'bob'), (1, 'alice');
-- input:
SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 ORDER BY id;
-- expected output:
1|alice
1|alice
2|bob
3|charlie
-- expected status: 0
