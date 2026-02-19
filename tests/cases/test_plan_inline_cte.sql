-- CTE inlining: single CTE with GROUP BY + HAVING + ORDER BY + LIMIT
-- setup:
CREATE TABLE events_ic (id INT, user_id INT, event_type INT, amount INT, score INT);
INSERT INTO events_ic VALUES (1, 1, 0, 100, 10);
INSERT INTO events_ic VALUES (2, 1, 0, 200, 20);
INSERT INTO events_ic VALUES (3, 1, 1, 500, 50);
INSERT INTO events_ic VALUES (4, 2, 0, 300, 30);
INSERT INTO events_ic VALUES (5, 2, 0, 400, 40);
INSERT INTO events_ic VALUES (6, 3, 0, 50, 5);
INSERT INTO events_ic VALUES (7, 3, 0, 60, 6);
INSERT INTO events_ic VALUES (8, 4, 0, 250, 25);
INSERT INTO events_ic VALUES (9, 4, 0, 350, 35);
-- input:
WITH user_totals AS (SELECT user_id, SUM(amount) AS total FROM events_ic WHERE event_type = 0 GROUP BY user_id HAVING SUM(amount) > 500) SELECT * FROM user_totals ORDER BY total DESC LIMIT 100;
-- expected output:
2|700
4|600
