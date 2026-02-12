-- WITH CTE INSERT INTO ... SELECT
-- setup:
CREATE TABLE src (id INT, val INT);
INSERT INTO src VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE dst (id INT, val INT);
-- input:
WITH filtered AS (SELECT id, val FROM src WHERE val >= 20) INSERT INTO dst SELECT id, val FROM filtered;
SELECT id, val FROM dst ORDER BY id;
-- expected output:
INSERT 0 2
2|20
3|30
-- expected status: 0
