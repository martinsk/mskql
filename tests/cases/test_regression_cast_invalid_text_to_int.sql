-- casting non-numeric text to int should error, not return 0
-- setup:
CREATE TABLE t (val TEXT);
INSERT INTO t VALUES ('abc');
-- input:
SELECT val::int FROM t;
-- expected output:
ERROR:  invalid input syntax for type integer: "abc"
-- expected status: 0
