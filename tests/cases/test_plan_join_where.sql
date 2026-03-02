-- test plan join where
-- setup:
CREATE TABLE test_jw1 (id INT, name TEXT);
CREATE TABLE test_jw2 (fk INT, amount INT);
INSERT INTO test_jw1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO test_jw2 VALUES (1, 300), (2, 100), (3, 200);
-- input:
SELECT t1.name, t2.amount FROM test_jw1 t1 JOIN test_jw2 t2 ON t1.id = t2.fk WHERE t2.amount > 150
EXPLAIN SELECT t1.name, t2.amount FROM test_jw1 t1 JOIN test_jw2 t2 ON t1.id = t2.fk WHERE t2.amount > 150

-- expected output:
alice|300
charlie|200
Project
  Hash Join
    Seq Scan on test_jw1
    Filter: (t2.amount > 150)
      Seq Scan on test_jw2

-- expected status: 0
