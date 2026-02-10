-- UPDATE SET with string concatenation expression
-- setup:
CREATE TABLE t1 (id INT, name TEXT, suffix TEXT);
INSERT INTO t1 (id, name, suffix) VALUES (1, 'alice', '_v2'), (2, 'bob', '_v2');
-- input:
UPDATE t1 SET name = name || suffix WHERE id = 1;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
UPDATE 1
1|alice_v2
2|bob
-- expected status: 0
