-- plan: JOIN with BETWEEN WHERE clause
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (fk INT, score INT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO t2 VALUES (1, 50), (2, 75), (3, 90);
-- input:
SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.score BETWEEN 60 AND 80 ORDER BY t1.name;
EXPLAIN SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.score BETWEEN 60 AND 80 ORDER BY t1.name
-- expected output:
bob
Project
  Sort
    Hash Join
      Seq Scan on t1
      Filter: (t2.score BETWEEN 60)
        Seq Scan on t2
-- expected status: 0
