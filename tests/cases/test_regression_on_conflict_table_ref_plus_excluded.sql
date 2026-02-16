-- bug: ON CONFLICT DO UPDATE SET with table.col + EXCLUDED.col ignores table.col value
-- setup:
CREATE TABLE oc_add (id INT PRIMARY KEY, val INT);
INSERT INTO oc_add VALUES (1, 10);
-- input:
INSERT INTO oc_add VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = oc_add.val + EXCLUDED.val;
SELECT * FROM oc_add;
-- expected output:
INSERT 0 1
1|30
