-- BETWEEN with text values should compare lexicographically
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave');
-- input:
SELECT id, name FROM t1 WHERE name BETWEEN 'b' AND 'd' ORDER BY id;
-- expected output:
2|bob
3|carol
-- expected status: 0
