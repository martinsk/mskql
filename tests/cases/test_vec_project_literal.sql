-- Test vectorized VEC_LITERAL: broadcast constants in SELECT list
CREATE TABLE t_lit (a INT, b TEXT);
INSERT INTO t_lit VALUES (1, 'hello');
INSERT INTO t_lit VALUES (2, 'world');
INSERT INTO t_lit VALUES (3, 'foo');

-- Integer literal alongside column ref
SELECT a, 42 FROM t_lit ORDER BY a;
-- expected: 1|42
-- expected: 2|42
-- expected: 3|42

-- Text literal alongside column ref
SELECT a, 'constant' FROM t_lit ORDER BY a;
-- expected: 1|constant
-- expected: 2|constant
-- expected: 3|constant

-- NULL literal
SELECT a, NULL FROM t_lit ORDER BY a;
-- expected: 1|
-- expected: 2|
-- expected: 3|

-- Float literal
SELECT a, 3.14 FROM t_lit ORDER BY a;
-- expected: 1|3.14
-- expected: 2|3.14
-- expected: 3|3.14
