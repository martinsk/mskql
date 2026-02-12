-- CTE used in INSERT ... SELECT
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
WITH src AS (SELECT id, name FROM t1 WHERE id = 1) INSERT INTO t2 SELECT id, name FROM src;
-- input:
SELECT id, name FROM t2;
-- expected output:
1|alice
-- expected status: 0
