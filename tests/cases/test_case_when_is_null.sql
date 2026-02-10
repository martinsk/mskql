-- CASE WHEN with IS NULL condition
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'bob');
-- input:
SELECT id, CASE WHEN name IS NULL THEN 'unknown' ELSE name END FROM t1 ORDER BY id;
-- expected output:
1|alice
2|unknown
3|bob
-- expected status: 0
