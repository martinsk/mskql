-- adversarial: REPLACE where replacement is longer than search — tests size calculation
-- setup:
CREATE TABLE t_repl (s TEXT);
INSERT INTO t_repl VALUES ('aaa');
-- input:
SELECT REPLACE(s, 'a', 'xyz') FROM t_repl;
-- expected output:
xyzxyzxyz
