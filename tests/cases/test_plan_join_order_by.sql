-- test plan join order by
-- setup:
CREATE TABLE test_jo1 (id INT, name TEXT);
CREATE TABLE test_jo2 (fk INT, amount INT);
INSERT INTO test_jo1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO test_jo2 VALUES (1, 300), (2, 100), (3, 200);
-- input:
SELECT t1.name, t2.amount FROM test_jo1 t1 JOIN test_jo2 t2 ON t1.id = t2.fk ORDER BY t2.amount
EXPLAIN SELECT t1.name, t2.amount FROM test_jo1 t1 JOIN test_jo2 t2 ON t1.id = t2.fk ORDER BY t2.amount

-- expected output:
bob|100
charlie|200
alice|300
Project
  Sort
    Hash Join
      Seq Scan on test_jo1
      Seq Scan on test_jo2

-- expected status: 0
