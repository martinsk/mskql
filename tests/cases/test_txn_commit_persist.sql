-- transaction COMMIT should persist changes
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
-- input:
BEGIN;
INSERT INTO t1 (id, name) VALUES (1, 'alice');
COMMIT;
SELECT id, name FROM t1;
-- expected output:
BEGIN
INSERT 0 1
COMMIT
1|alice
-- expected status: 0
