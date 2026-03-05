-- Test vectorized LPAD/RPAD
CREATE TABLE t_pad (s TEXT);
INSERT INTO t_pad VALUES ('hi');
INSERT INTO t_pad VALUES ('hello');
INSERT INTO t_pad VALUES ('a');

SELECT LPAD(s, 5, '*') FROM t_pad ORDER BY s;
-- expected: ****a
-- expected: ***hi
-- expected: hello

SELECT RPAD(s, 5, '-') FROM t_pad ORDER BY s;
-- expected: a----
-- expected: hi---
-- expected: hello

-- Default pad char is space
SELECT LPAD(s, 5) FROM t_pad ORDER BY s;
-- expected:     a
-- expected:    hi
-- expected: hello
