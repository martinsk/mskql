-- Test nested arithmetic expressions vectorized via VEC_PROJECT with aux ops
-- setup
CREATE TABLE test_nested_expr (a INT, b INT, c INT);
INSERT INTO test_nested_expr VALUES (10, 3, 2);
INSERT INTO test_nested_expr VALUES (20, 5, 4);
INSERT INTO test_nested_expr VALUES (100, 7, 6);
-- input
SELECT (a + b) * c FROM test_nested_expr;
-- expected
26
100
642
-- input
SELECT a * b + a * c FROM test_nested_expr;
-- expected
50
180
1300
-- input
SELECT (a + 1) * (b + 1) FROM test_nested_expr;
-- expected
44
126
808
-- input
SELECT a + b + c FROM test_nested_expr;
-- expected
15
29
113
-- input
SELECT (a - b) * c + 1 FROM test_nested_expr;
-- expected
15
61
559
