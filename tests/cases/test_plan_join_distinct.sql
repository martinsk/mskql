CREATE TABLE test_jd1 (id INT, name TEXT);
CREATE TABLE test_jd2 (fk INT, city TEXT);
INSERT INTO test_jd1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO test_jd2 VALUES (1, 'NYC'), (1, 'NYC'), (2, 'LA');
SELECT DISTINCT t1.name, t2.city FROM test_jd1 t1 JOIN test_jd2 t2 ON t1.id = t2.fk;
-- EXPECT: 2 rows (alice/NYC deduplicated)
-- alice|NYC
-- bob|LA
DROP TABLE test_jd2;
DROP TABLE test_jd1;
