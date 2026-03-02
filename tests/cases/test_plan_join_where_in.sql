-- plan: JOIN with IN-list WHERE clause
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, category TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- input:
SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.category IN ('a', 'c') ORDER BY t1.name;
EXPLAIN SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.category IN ('a', 'c') ORDER BY t1.name
-- expected output:
alice
charlie
Project
  Sort
    Hash Join
      Seq Scan on t1
      Filter: (t2.category IN 0)
        Seq Scan on t2
-- expected status: 0
