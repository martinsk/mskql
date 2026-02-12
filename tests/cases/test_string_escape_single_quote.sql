-- string literal with escaped single quote ('')
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'it''s');
-- input:
SELECT id, name FROM t1;
-- expected output:
1|it's
-- expected status: 0
