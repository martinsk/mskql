-- ILIKE should match case-insensitively
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'Alice'), (2, 'BOB'), (3, 'charlie');
-- input:
SELECT id, name FROM t1 WHERE name ILIKE 'alice' ORDER BY id;
-- expected output:
1|Alice
-- expected status: 0
