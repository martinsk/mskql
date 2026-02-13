-- adversarial: SELECT from nonexistent table — should error, not crash
-- input:
SELECT * FROM ghost_table;
-- expected status: error
