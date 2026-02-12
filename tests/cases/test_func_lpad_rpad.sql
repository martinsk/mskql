-- LPAD and RPAD functions
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hi'), (2, 'hello');
-- input:
SELECT id, LPAD(name, 5, '*'), RPAD(name, 5, '-') FROM t1 ORDER BY id;
-- expected output:
1|***hi|hi---
2|hello|hello
-- expected status: 0
