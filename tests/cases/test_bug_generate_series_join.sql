-- bug: generate_series temp table missing flat storage — JOIN returns empty
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT t.id, t.name FROM generate_series(1, 3) AS gs JOIN t ON t.id = gs.generate_series ORDER BY t.id;
-- expected output:
1|alice
2|bob
3|charlie
-- expected status: 0
