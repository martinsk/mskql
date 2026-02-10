-- COALESCE combined with string concatenation
-- setup:
CREATE TABLE t1 (id INT, first TEXT, last TEXT);
INSERT INTO t1 (id, first, last) VALUES (1, 'Alice', 'Smith'), (2, NULL, 'Jones');
-- input:
SELECT id, COALESCE(first, 'Unknown') || ' ' || last FROM t1 ORDER BY id;
-- expected output:
1|Alice Smith
2|Unknown Jones
-- expected status: 0
