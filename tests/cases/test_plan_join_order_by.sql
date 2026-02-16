CREATE TABLE test_jo1 (id INT, name TEXT);
CREATE TABLE test_jo2 (fk INT, amount INT);
INSERT INTO test_jo1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
INSERT INTO test_jo2 VALUES (1, 300), (2, 100), (3, 200);
SELECT t1.name, t2.amount FROM test_jo1 t1 JOIN test_jo2 t2 ON t1.id = t2.fk ORDER BY t2.amount;
-- EXPECT:
-- bob|100
-- charlie|200
-- alice|300
DROP TABLE test_jo2;
DROP TABLE test_jo1;
