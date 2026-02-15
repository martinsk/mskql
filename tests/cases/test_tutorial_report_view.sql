-- tutorial: reporting dashboard - pipeline summary query (reporting-dashboard.html step 7)
-- setup:
CREATE TABLE reps (id SERIAL PRIMARY KEY, name TEXT NOT NULL, region TEXT NOT NULL);
CREATE TABLE deals (id SERIAL PRIMARY KEY, rep_id INT NOT NULL REFERENCES reps(id), customer TEXT NOT NULL, amount INT NOT NULL, stage TEXT NOT NULL DEFAULT 'prospect', closed DATE);
INSERT INTO reps (name, region) VALUES ('Alice', 'West'), ('Bob', 'East'), ('Carol', 'West'), ('Dave', 'East'), ('Eve', 'Central');
INSERT INTO deals (rep_id, customer, amount, stage, closed) VALUES (1, 'Acme Corp', 45000, 'won', '2025-01-10'), (1, 'Globex', 30000, 'won', '2025-01-18'), (1, 'Initech', 20000, 'prospect', NULL), (2, 'Umbrella', 55000, 'won', '2025-01-05'), (2, 'Stark Ind', 40000, 'lost', NULL), (3, 'Wayne Ent', 60000, 'won', '2025-01-22'), (3, 'Oscorp', 35000, 'prospect', NULL), (4, 'LexCorp', 25000, 'won', '2025-01-15'), (4, 'Cyberdyne', 70000, 'won', '2025-01-28'), (5, 'Aperture', 50000, 'won', '2025-01-12'), (5, 'Weyland-Yutani', 15000, 'lost', NULL);
-- input:
SELECT r.name AS rep, r.region, SUM(CASE WHEN d.stage = 'won' THEN 1 ELSE 0 END) AS won, SUM(d.amount) AS pipeline, SUM(CASE WHEN d.stage = 'won' THEN d.amount ELSE 0 END) AS revenue FROM deals d JOIN reps r ON d.rep_id = r.id GROUP BY r.name, r.region ORDER BY revenue DESC;
-- expected output:
Dave|East|2|95000|95000
Alice|West|2|95000|75000
Carol|West|1|95000|60000
Bob|East|1|95000|55000
Eve|Central|1|65000|50000
-- expected status: 0
