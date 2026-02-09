-- transaction left open by previous connection is rolled back on disconnect
-- The setup opens a transaction and inserts a row but never commits.
-- When the setup session ends, the server rolls back the open transaction.
-- The input SELECT (on a new connection) should NOT see the uncommitted row.
-- setup:
CREATE TABLE "txn_test" (id INT, val TEXT);
INSERT INTO "txn_test" (id, val) VALUES (1, 'before');
BEGIN;
INSERT INTO "txn_test" (id, val) VALUES (2, 'in_txn');
-- input:
SELECT * FROM "txn_test" ORDER BY id;
-- expected output:
1|before
-- expected status: 0
