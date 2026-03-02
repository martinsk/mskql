-- explain parse: simple SELECT with WHERE and ORDER BY
-- setup:
CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT);
INSERT INTO emp VALUES (1,'Alice','eng',90000),(2,'Bob','sales',70000),(3,'Carol','eng',110000);
-- input:
EXPLAIN (PARSE) SELECT name, salary FROM emp WHERE dept = 'eng' ORDER BY salary DESC
-- expected output:
Parse AST (SELECT)
  FROM:      emp
  COLUMNS:   name, salary
  WHERE:     dept = 'eng'
  ORDER BY:  salary DESC
-- expected status: 0
