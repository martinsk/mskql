-- tutorial: indexes - primary key lookup (indexes-performance.html)
-- setup:
CREATE TABLE events (id SERIAL PRIMARY KEY, user_id INT NOT NULL, action TEXT NOT NULL, amount INT, created DATE NOT NULL);
INSERT INTO events (user_id, action, amount, created) VALUES (1, 'login', NULL, '2025-01-01'), (1, 'purchase', 50, '2025-01-02'), (2, 'login', NULL, '2025-01-01'), (2, 'purchase', 30, '2025-01-03'), (3, 'login', NULL, '2025-01-02'), (3, 'purchase', 200, '2025-01-04'), (3, 'purchase', 75, '2025-01-07'), (3, 'refund', -75, '2025-01-08');
-- input:
SELECT id, user_id, action, amount FROM events WHERE id = 6;
-- expected output:
6|3|purchase|200
-- expected status: 0
