-- explain all: outputs parse + logical + physical separated by ---
-- setup:
CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT);
INSERT INTO emp VALUES (1,'Alice','eng',90000),(2,'Bob','sales',70000),(3,'Carol','eng',110000);
-- input:
EXPLAIN (ALL) SELECT name, salary FROM emp WHERE dept = 'eng' ORDER BY salary DESC
-- expected output:
Parse AST (SELECT)
  FROM:      emp
  COLUMNS:   name, salary
  WHERE:     dept = 'eng'
  ORDER BY:  salary DESC
---
Sort [salary DESC]
  Project [name, salary]
    Filter: (dept = 'eng')
      Scan on emp
---
Project
  Sort (salary DESC)
    Filter: (dept = 'eng')
      Seq Scan on emp
-- expected status: 0
