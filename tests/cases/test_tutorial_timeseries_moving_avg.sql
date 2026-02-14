-- tutorial: time-series - moving average window frame (time-series.html step 6)
-- setup:
CREATE TABLE sensors (id SERIAL PRIMARY KEY, name TEXT NOT NULL, location TEXT NOT NULL);
CREATE TABLE readings (id SERIAL PRIMARY KEY, sensor_id INT NOT NULL REFERENCES sensors(id), recorded TIMESTAMP NOT NULL, temp_c INT NOT NULL, humidity INT);
INSERT INTO sensors (name, location) VALUES ('TH-01', 'Warehouse A'), ('TH-02', 'Warehouse B'), ('TH-03', 'Office');
INSERT INTO readings (sensor_id, recorded, temp_c, humidity) VALUES (1, '2025-01-10 08:00:00', 18, 45), (1, '2025-01-10 12:00:00', 22, 40), (1, '2025-01-10 18:00:00', 20, 42), (1, '2025-01-11 08:00:00', 17, 48), (1, '2025-01-11 12:00:00', 23, 38), (1, '2025-01-11 18:00:00', 21, 41), (1, '2025-01-12 08:00:00', 19, 44), (1, '2025-01-12 12:00:00', 24, 36);
-- input:
SELECT r.recorded, r.temp_c, AVG(r.temp_c) OVER (ORDER BY r.recorded ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg FROM readings r WHERE r.sensor_id = 1 ORDER BY r.recorded;
-- expected output:
2025-01-10 08:00:00|18|18
2025-01-10 12:00:00|22|20
2025-01-10 18:00:00|20|20
2025-01-11 08:00:00|17|19.6667
2025-01-11 12:00:00|23|20
2025-01-11 18:00:00|21|20.3333
2025-01-12 08:00:00|19|21
2025-01-12 12:00:00|24|21.3333
-- expected status: 0
