-- test plan join distinct
-- setup:
CREATE TABLE test_jd1 (id INT, name TEXT);
CREATE TABLE test_jd2 (fk INT, city TEXT);
INSERT INTO test_jd1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO test_jd2 VALUES (1, 'NYC'), (1, 'NYC'), (2, 'LA');
-- input:
SELECT DISTINCT t1.name, t2.city FROM test_jd1 t1 JOIN test_jd2 t2 ON t1.id = t2.fk
EXPLAIN SELECT DISTINCT t1.name, t2.city FROM test_jd1 t1 JOIN test_jd2 t2 ON t1.id = t2.fk

-- expected output:
alice|NYC
bob|LA
HashAggregate (DISTINCT)
  Project
    Hash Join
      Seq Scan on test_jd1
      Seq Scan on test_jd2

-- expected status: 0
