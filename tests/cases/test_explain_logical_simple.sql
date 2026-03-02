-- explain logical: simple SELECT with WHERE and ORDER BY
-- setup:
CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT);
INSERT INTO emp VALUES (1,'Alice','eng',90000),(2,'Bob','sales',70000),(3,'Carol','eng',110000);
-- input:
EXPLAIN (LOGICAL) SELECT name, salary FROM emp WHERE dept = 'eng' ORDER BY salary DESC
-- expected output:
Sort [salary DESC]
  Project [name, salary]
    Filter: (dept = 'eng')
      Scan on emp
-- expected status: 0
