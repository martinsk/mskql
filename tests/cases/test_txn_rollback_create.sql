-- ROLLBACK should undo CREATE TABLE
-- setup:
CREATE TABLE existing (id INT);
-- input:
BEGIN;
CREATE TABLE new_table (id INT, name TEXT);
INSERT INTO new_table (id, name) VALUES (1, 'test');
ROLLBACK;
SELECT * FROM new_table;
-- expected status: 1
