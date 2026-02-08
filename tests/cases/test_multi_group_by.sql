-- multi-column group by
-- setup:
CREATE TABLE "t1" (dept TEXT, role TEXT, val INT);
INSERT INTO "t1" (dept, role, val) VALUES ('a', 'dev', 10), ('a', 'dev', 20), ('a', 'mgr', 30), ('b', 'dev', 40);
-- input:
SELECT dept, role, SUM(val) FROM "t1" GROUP BY dept, role ORDER BY dept, role;
-- expected output:
a|dev|30
a|mgr|30
b|dev|40
-- expected status: 0
