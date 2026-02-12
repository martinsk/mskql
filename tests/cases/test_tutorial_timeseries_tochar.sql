-- tutorial: time-series TO_CHAR formatting (time-series.html)
-- setup:
CREATE TABLE sensors (id SERIAL PRIMARY KEY, name TEXT NOT NULL, location TEXT NOT NULL);
CREATE TABLE readings (id SERIAL PRIMARY KEY, sensor_id INT NOT NULL REFERENCES sensors(id), recorded TIMESTAMP NOT NULL, temp_c INT NOT NULL, humidity INT);
INSERT INTO sensors (name, location) VALUES ('TH-01', 'Warehouse A'), ('TH-02', 'Warehouse B'), ('TH-03', 'Office');
INSERT INTO readings (sensor_id, recorded, temp_c, humidity) VALUES (3, '2025-01-10 09:00:00', 21, 35), (3, '2025-01-10 14:00:00', 23, 33), (3, '2025-01-11 09:00:00', 22, 34), (3, '2025-01-11 14:00:00', 24, 32);
-- input:
SELECT s.name, TO_CHAR(r.recorded, 'YYYY-MM-DD HH24:MI') AS time, r.temp_c FROM readings r JOIN sensors s ON r.sensor_id = s.id WHERE s.name = 'TH-03' ORDER BY r.recorded;
-- expected output:
TH-03|2025-01-10 09:00:00|21
TH-03|2025-01-10 14:00:00|23
TH-03|2025-01-11 09:00:00|22
TH-03|2025-01-11 14:00:00|24
-- expected status: 0
