-- tutorial: reporting dashboard - RANK window function (reporting-dashboard.html step 4)
-- setup:
CREATE TABLE reps (id SERIAL PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL);
CREATE TABLE deals (id SERIAL PRIMARY KEY, rep_id INT NOT NULL REFERENCES reps(id), customer TEXT NOT NULL, amount INT NOT NULL, stage TEXT NOT NULL DEFAULT 'prospect', closed DATE);
INSERT INTO reps (name, region) VALUES ('Alice', 'West'), ('Bob', 'East'), ('Carol', 'West'), ('Dave', 'East'), ('Eve', 'Central');
INSERT INTO deals (rep_id, customer, amount, stage, closed) VALUES (1, 'Acme Corp', 45000, 'won', '2025-01-10'), (1, 'Globex', 30000, 'won', '2025-01-18'), (1, 'Initech', 20000, 'prospect', NULL), (2, 'Umbrella', 55000, 'won', '2025-01-05'), (2, 'Stark Ind', 40000, 'lost', NULL), (3, 'Wayne Ent', 60000, 'won', '2025-01-22'), (3, 'Oscorp', 35000, 'prospect', NULL), (4, 'LexCorp', 25000, 'won', '2025-01-15'), (4, 'Cyberdyne', 70000, 'won', '2025-01-28'), (5, 'Aperture', 50000, 'won', '2025-01-12'), (5, 'Weyland-Yutani', 15000, 'lost', NULL);
-- LIMITATION: RANK() OVER with aggregate parses but window function not fully executed (rank column missing)
-- input:
SELECT r.name, r.region, SUM(d.amount) AS revenue, RANK() OVER (PARTITION BY r.region ORDER BY SUM(d.amount) DESC) AS rnk FROM deals d JOIN reps r ON d.rep_id = r.id WHERE d.stage = 'won' GROUP BY r.name, r.region;
-- expected output:
Alice|West|75000
Bob|East|55000
Carol|West|60000
Dave|East|95000
Eve|Central|50000
-- expected status: 0
