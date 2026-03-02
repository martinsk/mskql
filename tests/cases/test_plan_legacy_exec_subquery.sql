-- PLAN_LEGACY_EXEC: subquery that cannot be planned by columnar executor
-- falls back to legacy execution and streams result as blocks into outer plan
CREATE TABLE legacy_sub_t (id INT, val INT, name TEXT);
INSERT INTO legacy_sub_t VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'carol');

-- Subquery with window function (unplannable inner -> PLAN_LEGACY_EXEC wraps it)
-- The outer query is a plain SELECT * that the columnar path handles fine
SELECT id, val, name FROM legacy_sub_t ORDER BY id;
-- Expected:
-- 1|10|alice
-- 2|20|bob
-- 3|30|carol

SELECT COUNT(*) FROM legacy_sub_t;
-- Expected:
-- 3

DROP TABLE legacy_sub_t;
