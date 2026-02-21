-- BUG: Regex operators (~, ~*, !~, !~*) not supported in SELECT expressions, only in WHERE
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'hello');
-- input:
SELECT id, val ~ 'hel' AS matches FROM t;
-- expected output:
1|t
-- expected status: 0
