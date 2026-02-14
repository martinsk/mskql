-- tutorial: reporting dashboard - HAVING filter (reporting-dashboard.html step 3)
-- setup:
CREATE TABLE reps (id SERIAL PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL);
CREATE TABLE deals (id SERIAL PRIMARY KEY, rep_id INT NOT NULL REFERENCES reps(id), customer TEXT NOT NULL, amount INT NOT NULL, stage TEXT NOT NULL DEFAULT 'prospect', closed DATE);
INSERT INTO reps (name, region) VALUES ('Alice', 'West'), ('Bob', 'East'), ('Carol', 'West'), ('Dave', 'East'), ('Eve', 'Central');
INSERT INTO deals (rep_id, customer, amount, stage, closed) VALUES (1, 'Acme Corp', 45000, 'won', '2025-01-10'), (1, 'Globex', 30000, 'won', '2025-01-18'), (1, 'Initech', 20000, 'prospect', NULL), (2, 'Umbrella', 55000, 'won', '2025-01-05'), (2, 'Stark Ind', 40000, 'lost', NULL), (3, 'Wayne Ent', 60000, 'won', '2025-01-22'), (3, 'Oscorp', 35000, 'prospect', NULL), (4, 'LexCorp', 25000, 'won', '2025-01-15'), (4, 'Cyberdyne', 70000, 'won', '2025-01-28'), (5, 'Aperture', 50000, 'won', '2025-01-12'), (5, 'Weyland-Yutani', 15000, 'lost', NULL);
-- input:
SELECT r.name, SUM(d.amount) AS pipeline FROM deals d JOIN reps r ON d.rep_id = r.id GROUP BY r.name HAVING SUM(d.amount) > 60000 ORDER BY pipeline DESC;
-- expected output:
Eve|65000
Alice|95000
Bob|95000
Carol|95000
Dave|95000
-- expected status: 0
