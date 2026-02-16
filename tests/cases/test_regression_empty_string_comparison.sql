-- bug: comparing column to empty string '' fails with "expected FROM, got '='"
-- setup:
CREATE TABLE t_esc (id INT, val TEXT);
INSERT INTO t_esc VALUES (1, ''), (2, NULL), (3, 'hello');
-- input:
SELECT id FROM t_esc WHERE val = '' ORDER BY id;
-- expected output:
1
