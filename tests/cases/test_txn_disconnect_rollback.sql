-- transaction should be rolled back when client disconnects
-- A BEGIN in setup runs on a separate psql connection that disconnects.
-- The input SELECT runs on a new connection and should NOT see uncommitted data.
-- setup:
CREATE TABLE "txn_disc" (id INT, val TEXT);
INSERT INTO "txn_disc" (id, val) VALUES (1, 'committed');
BEGIN;
INSERT INTO "txn_disc" (id, val) VALUES (2, 'uncommitted');
-- input:
SELECT * FROM "txn_disc" ORDER BY id;
-- expected output:
1|committed
-- expected status: 0
