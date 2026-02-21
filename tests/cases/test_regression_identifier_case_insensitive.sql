-- BUG: Unquoted identifiers are case-sensitive (should be case-insensitive per SQL standard)
-- setup:
CREATE TABLE MyTable (id INT, val TEXT);
INSERT INTO mytable VALUES (1, 'test');
-- input:
SELECT * FROM MYTABLE;
-- expected output:
1|test
-- expected status: 0
