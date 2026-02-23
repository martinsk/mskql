-- analytics stress: parquet + in-memory join + scalar functions (ABS, ROUND) + GROUP BY
-- setup:
CREATE FOREIGN TABLE amj_orders OPTIONS (FILENAME '@@FIXTURES@@/mini_orders.parquet');
CREATE TABLE amj_returns (id INT, order_id INT, reason TEXT, refund_amount INT);
INSERT INTO amj_returns VALUES (0, 0, 'defective', 80);
INSERT INTO amj_returns VALUES (1, 1, 'wrong_size', 250);
INSERT INTO amj_returns VALUES (2, 2, 'defective', 30);
INSERT INTO amj_returns VALUES (3, 3, 'damaged', 350);
INSERT INTO amj_returns VALUES (4, 4, 'wrong_size', 100);
-- input:
SELECT r.reason, COUNT(*), SUM(ABS(r.refund_amount - o.amount)) FROM amj_returns r JOIN amj_orders o ON r.order_id = o.id GROUP BY r.reason ORDER BY r.reason;
-- expected output:
damaged|1|50
defective|2|40
wrong_size|2|100
-- expected status: 0
