-- adversarial: NULL propagation through arithmetic and concatenation
-- BUG: c || ' world' where c='hello' returns 'hello world' (correct),
-- but NULL || ' world' should return NULL per SQL standard.
-- Currently mskql returns 'hello world' for row 1 (correct) but the
-- NULL propagation through || is correct for row 2.
-- setup:
CREATE TABLE t_na (a INT, b INT, c TEXT);
INSERT INTO t_na VALUES (1, NULL, 'hello');
INSERT INTO t_na VALUES (NULL, 2, NULL);
-- input:
SELECT a + b, a * b, a - b, c || ' world' FROM t_na;
-- expected output:
|||hello world
|||
