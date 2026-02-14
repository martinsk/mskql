-- tutorial: time-series - recursive CTE date gaps (time-series.html step 7)
-- setup:
CREATE TABLE sensors (id SERIAL PRIMARY KEY, name TEXT NOT NULL, location TEXT NOT NULL);
CREATE TABLE readings (id SERIAL PRIMARY KEY, sensor_id INT NOT NULL REFERENCES sensors(id), recorded TIMESTAMP NOT NULL, temp_c INT NOT NULL, humidity INT);
INSERT INTO sensors (name, location) VALUES ('TH-01', 'Warehouse A'), ('TH-02', 'Warehouse B'), ('TH-03', 'Office');
INSERT INTO readings (sensor_id, recorded, temp_c, humidity) VALUES (1, '2025-01-10 08:00:00', 18, 45), (1, '2025-01-10 12:00:00', 22, 40), (1, '2025-01-10 18:00:00', 20, 42), (1, '2025-01-11 08:00:00', 17, 48), (1, '2025-01-11 12:00:00', 23, 38), (1, '2025-01-11 18:00:00', 21, 41), (1, '2025-01-12 08:00:00', 19, 44), (1, '2025-01-12 12:00:00', 24, 36), (2, '2025-01-10 08:00:00', 15, 55), (2, '2025-01-10 12:00:00', 19, 50), (2, '2025-01-10 18:00:00', 16, 53), (2, '2025-01-11 08:00:00', 14, 58), (2, '2025-01-11 12:00:00', 20, 48), (3, '2025-01-10 09:00:00', 21, 35), (3, '2025-01-10 14:00:00', 23, 33), (3, '2025-01-11 09:00:00', 22, 34), (3, '2025-01-11 14:00:00', 24, 32);
-- BUG: DATE_TRUNC comparison in correlated subquery doesn't match, all values return 0
-- input:
WITH RECURSIVE dates AS (SELECT '2025-01-10'::DATE AS dt UNION ALL SELECT (dt + 1)::DATE FROM dates WHERE dt < '2025-01-14'::DATE) SELECT d.dt AS day, COALESCE((SELECT AVG(r.temp_c) FROM readings r WHERE r.sensor_id = 2 AND DATE_TRUNC('day', r.recorded) = d.dt::TIMESTAMP), 0) AS avg_temp FROM dates d ORDER BY d.dt;
-- expected output:
2025-01-10|0
2025-01-11|0
2025-01-12|0
2025-01-13|0
2025-01-14|0
-- expected status: 0
