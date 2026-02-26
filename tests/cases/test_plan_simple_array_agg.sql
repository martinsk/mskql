-- plan executor: ARRAY_AGG without GROUP BY (simple aggregate)
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
-- input:
SELECT ARRAY_AGG(name) FROM t;
-- expected output:
{alice,bob,carol}
