-- Concatenating int column with text should work via || operator
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'item');
-- input:
SELECT name || '-' || id FROM t1;
-- expected output:
item-1
-- expected status: 0
