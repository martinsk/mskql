-- adversarial: DROP TABLE IF EXISTS on nonexistent table — should succeed silently
-- input:
DROP TABLE IF EXISTS this_table_does_not_exist;
-- expected output:
