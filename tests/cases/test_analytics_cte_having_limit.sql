-- analytics stress: CTE + GROUP BY + HAVING + ORDER BY + LIMIT
-- setup:
CREATE TABLE ach_events (id INT, user_id INT, event_type INT, amount INT, score INT);
INSERT INTO ach_events VALUES (0, 0, 0, 100, 9000);
INSERT INTO ach_events VALUES (1, 0, 0, 200, 8500);
INSERT INTO ach_events VALUES (2, 0, 1, 50, 7000);
INSERT INTO ach_events VALUES (3, 1, 0, 300, 9500);
INSERT INTO ach_events VALUES (4, 1, 0, 400, 8000);
INSERT INTO ach_events VALUES (5, 1, 1, 60, 6000);
INSERT INTO ach_events VALUES (6, 2, 0, 10, 7500);
INSERT INTO ach_events VALUES (7, 2, 0, 20, 8200);
INSERT INTO ach_events VALUES (8, 2, 1, 30, 5000);
INSERT INTO ach_events VALUES (9, 0, 0, 150, 9100);
-- input:
WITH user_totals AS (SELECT user_id, SUM(amount) AS total, COUNT(*) AS cnt FROM ach_events WHERE event_type = 0 GROUP BY user_id HAVING SUM(amount) > 500) SELECT * FROM user_totals ORDER BY total DESC LIMIT 50;
-- expected output:
1|700|2
-- expected status: 0
