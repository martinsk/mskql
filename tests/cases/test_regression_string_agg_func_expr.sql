-- bug: STRING_AGG with function expression argument returns empty instead of concatenated values
-- setup:
CREATE TABLE t_sa_func (id INT, grp TEXT, val TEXT);
INSERT INTO t_sa_func VALUES (1,'a','hello'),(2,'a','world'),(3,'b','foo');
-- input:
SELECT grp, STRING_AGG(UPPER(val), '-') FROM t_sa_func GROUP BY grp ORDER BY grp;
-- expected output:
a|HELLO-WORLD
b|FOO
-- expected status: 0
