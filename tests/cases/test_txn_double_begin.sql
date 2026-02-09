-- double BEGIN should not lose the original rollback point
-- The second BEGIN is ignored (warned), so ROLLBACK should still
-- restore to the state before the first BEGIN.
-- setup:
CREATE TABLE "txn_dbl" (id INT, val TEXT);
INSERT INTO "txn_dbl" (id, val) VALUES (1, 'original');
BEGIN;
INSERT INTO "txn_dbl" (id, val) VALUES (2, 'first_txn');
BEGIN;
INSERT INTO "txn_dbl" (id, val) VALUES (3, 'after_second_begin');
ROLLBACK;
-- input:
SELECT * FROM "txn_dbl" ORDER BY id;
-- expected output:
1|original
-- expected status: 0
