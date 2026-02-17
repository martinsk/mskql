-- tutorial: indexes - create index and query (indexes-performance.html)
-- setup:
CREATE TABLE events (id SERIAL PRIMARY KEY, user_id INT NOT NULL, action TEXT NOT NULL, amount INT, created DATE NOT NULL);
INSERT INTO events (user_id, action, amount, created) VALUES (1, 'login', NULL, '2025-01-01'), (1, 'purchase', 50, '2025-01-02'), (1, 'purchase', 120, '2025-01-05'), (2, 'login', NULL, '2025-01-01'), (2, 'purchase', 30, '2025-01-03'), (2, 'login', NULL, '2025-01-06'), (3, 'login', NULL, '2025-01-02'), (3, 'purchase', 200, '2025-01-04'), (3, 'purchase', 75, '2025-01-07'), (3, 'refund', -75, '2025-01-08'), (4, 'login', NULL, '2025-01-01'), (4, 'purchase', 90, '2025-01-02'), (4, 'purchase', 45, '2025-01-05'), (4, 'login', NULL, '2025-01-09'), (5, 'login', NULL, '2025-01-03'), (5, 'purchase', 300, '2025-01-04'), (5, 'refund', -100, '2025-01-06'), (5, 'purchase', 60, '2025-01-08'), (5, 'login', NULL, '2025-01-10'), (1, 'login', NULL, '2025-01-10');
CREATE INDEX idx_events_user ON events (user_id);
-- input:
SELECT id, action, amount, created FROM events WHERE user_id = 3 ORDER BY created;
-- expected output:
7|login||2025-01-02
8|purchase|200|2025-01-04
9|purchase|75|2025-01-07
10|refund|-75|2025-01-08
-- expected status: 0
