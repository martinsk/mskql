-- tutorial: time-series - EXTRACT hourly breakdown (time-series.html step 3)
-- setup:
CREATE TABLE sensors (id SERIAL PRIMARY KEY, name TEXT NOT NULL, location TEXT NOT NULL);
CREATE TABLE readings (id SERIAL PRIMARY KEY, sensor_id INT NOT NULL REFERENCES sensors(id), recorded TIMESTAMP NOT NULL, temp_c INT NOT NULL, humidity INT);
INSERT INTO sensors (name, location) VALUES ('TH-01', 'Warehouse A'), ('TH-02', 'Warehouse B'), ('TH-03', 'Office');
INSERT INTO readings (sensor_id, recorded, temp_c, humidity) VALUES (1, '2025-01-10 08:00:00', 18, 45), (1, '2025-01-10 12:00:00', 22, 40), (1, '2025-01-10 18:00:00', 20, 42), (1, '2025-01-11 08:00:00', 17, 48), (1, '2025-01-11 12:00:00', 23, 38), (1, '2025-01-11 18:00:00', 21, 41), (1, '2025-01-12 08:00:00', 19, 44), (1, '2025-01-12 12:00:00', 24, 36), (2, '2025-01-10 08:00:00', 15, 55), (2, '2025-01-10 12:00:00', 19, 50), (2, '2025-01-10 18:00:00', 16, 53), (2, '2025-01-11 08:00:00', 14, 58), (2, '2025-01-11 12:00:00', 20, 48), (3, '2025-01-10 09:00:00', 21, 35), (3, '2025-01-10 14:00:00', 23, 33), (3, '2025-01-11 09:00:00', 22, 34), (3, '2025-01-11 14:00:00', 24, 32);
-- input:
SELECT EXTRACT(HOUR FROM r.recorded) AS hour, AVG(r.temp_c) AS avg_temp, COUNT(*) AS readings FROM readings r JOIN sensors s ON r.sensor_id = s.id WHERE s.location = 'Warehouse A' GROUP BY EXTRACT(HOUR FROM r.recorded) ORDER BY hour;
-- expected output:
8|18|3
12|23|3
18|20.5|2
-- expected status: 0
