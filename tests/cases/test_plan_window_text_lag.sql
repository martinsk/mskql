CREATE TABLE test_wt (id INT, name TEXT);
INSERT INTO test_wt VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
SELECT name, LAG(name) OVER (ORDER BY id) AS prev_name FROM test_wt;
-- EXPECT:
-- alice|
-- bob|alice
-- charlie|bob
DROP TABLE test_wt;
