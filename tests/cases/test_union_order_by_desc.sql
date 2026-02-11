-- UNION with ORDER BY DESC
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (3, 'charlie');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (2, 'bob'), (4, 'dave');
-- input:
SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id DESC;
-- expected output:
4|dave
3|charlie
2|bob
1|alice
-- expected status: 0
