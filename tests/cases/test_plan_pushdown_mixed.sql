-- plan: pushdown mixed predicates join
-- setup:
CREATE TABLE pd_mix_emp (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT);
INSERT INTO pd_mix_emp VALUES (1, 'Alice', 'eng', 90000);
INSERT INTO pd_mix_emp VALUES (2, 'Bob', 'sales', 70000);
INSERT INTO pd_mix_emp VALUES (3, 'Carol', 'eng', 110000);
INSERT INTO pd_mix_emp VALUES (4, 'Dave', 'sales', 80000);
CREATE TABLE pd_mix_dept (id INT PRIMARY KEY, dept TEXT, budget INT);
INSERT INTO pd_mix_dept VALUES (1, 'eng', 500000);
INSERT INTO pd_mix_dept VALUES (2, 'sales', 300000);
-- input:
SELECT e.name, e.salary, d.budget FROM pd_mix_emp e JOIN pd_mix_dept d ON e.dept = d.dept WHERE e.dept = 'eng' AND d.budget > 400000 ORDER BY e.name
EXPLAIN SELECT e.name, e.salary, d.budget FROM pd_mix_emp e JOIN pd_mix_dept d ON e.dept = d.dept WHERE e.dept = 'eng' AND d.budget > 400000 ORDER BY e.name
-- expected output:
Alice|90000|500000
Carol|110000|500000
Project
  Sort
    Hash Join
      Filter: (e.dept = 'eng')
        Seq Scan on pd_mix_emp
      Filter: (d.budget > 400000)
        Seq Scan on pd_mix_dept
-- expected status: 0
