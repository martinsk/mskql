-- nested BEGIN should not break the transaction
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
BEGIN;
INSERT INTO t1 (id, name) VALUES (2, 'bob');
BEGIN;
INSERT INTO t1 (id, name) VALUES (3, 'charlie');
COMMIT;
-- input:
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
1|alice
2|bob
3|charlie
-- expected status: 0
