-- adversarial: DROP TABLE that doesn't exist — should error
-- input:
DROP TABLE this_table_does_not_exist;
-- expected status: error
