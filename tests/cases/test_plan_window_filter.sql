-- Test window function FILTER clause via plan executor
-- setup:
CREATE TABLE wf_sales (id INT, category TEXT, amount INT);
INSERT INTO wf_sales VALUES (1, 'A', 100);
INSERT INTO wf_sales VALUES (2, 'A', 200);
INSERT INTO wf_sales VALUES (3, 'A', -50);
INSERT INTO wf_sales VALUES (4, 'B', 300);
INSERT INTO wf_sales VALUES (5, 'B', -100);
INSERT INTO wf_sales VALUES (6, 'B', 150);
-- input:
SELECT id, category, amount, SUM(amount) FILTER (WHERE amount > 0) OVER (PARTITION BY category) AS pos_sum FROM wf_sales ORDER BY id;
-- expected output:
1|A|100|300
2|A|200|300
3|A|-50|300
4|B|300|450
5|B|-100|450
6|B|150|450
