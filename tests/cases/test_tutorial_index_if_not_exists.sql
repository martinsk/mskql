-- tutorial: indexes - IF NOT EXISTS (indexes-performance.html)
-- setup:
CREATE TABLE events (id SERIAL PRIMARY KEY, user_id INT NOT NULL, action TEXT NOT NULL, amount INT, created DATE NOT NULL);
INSERT INTO events (user_id, action, amount, created) VALUES (1, 'login', NULL, '2025-01-01'), (1, 'purchase', 50, '2025-01-02'), (2, 'login', NULL, '2025-01-01'), (2, 'purchase', 30, '2025-01-03'), (3, 'login', NULL, '2025-01-02'), (3, 'purchase', 200, '2025-01-04');
CREATE INDEX idx_events_user ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_user ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_action ON events (action);
-- input:
SELECT user_id, SUM(amount) AS total FROM events WHERE action = 'purchase' GROUP BY user_id ORDER BY total DESC;
-- expected output:
3|200
1|50
2|30
-- expected status: 0
