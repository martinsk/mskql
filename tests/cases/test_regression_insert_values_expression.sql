-- BUG: Expressions with operators (e.g. string concatenation) in INSERT VALUES not supported
-- setup:
CREATE TABLE t (id INT, val TEXT);
-- input:
INSERT INTO t VALUES (1, 'hello' || ' world');
-- expected output:
INSERT 0 1
-- expected status: 0
