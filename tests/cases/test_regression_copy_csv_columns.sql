-- REGRESSION: COPY FROM STDIN WITH CSV does not parse columns correctly (all columns after first are NULL)
-- setup:
CREATE TABLE t (id INT, name TEXT);
COPY t FROM '@@FIXTURES@@/copy_csv.csv' WITH (FORMAT csv);
-- input:
SELECT * FROM t ORDER BY id;
-- expected output:
1|Alice
2|Bob
-- expected status: 0
