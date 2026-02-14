-- tutorial: time-series - DATE_TRUNC daily averages (time-series.html step 2)
-- setup:
CREATE TABLE sensors (id SERIAL PRIMARY KEY, name TEXT NOT NULL, location TEXT NOT NULL);
CREATE TABLE readings (id SERIAL PRIMARY KEY, sensor_id INT NOT NULL REFERENCES sensors(id), recorded TIMESTAMP NOT NULL, temp_c INT NOT NULL, humidity INT);
INSERT INTO sensors (name, location) VALUES ('TH-01', 'Warehouse A'), ('TH-02', 'Warehouse B'), ('TH-03', 'Office');
INSERT INTO readings (sensor_id, recorded, temp_c, humidity) VALUES (1, '2025-01-10 08:00:00', 18, 45), (1, '2025-01-10 12:00:00', 22, 40), (1, '2025-01-10 18:00:00', 20, 42), (1, '2025-01-11 08:00:00', 17, 48), (1, '2025-01-11 12:00:00', 23, 38), (1, '2025-01-11 18:00:00', 21, 41), (1, '2025-01-12 08:00:00', 19, 44), (1, '2025-01-12 12:00:00', 24, 36), (2, '2025-01-10 08:00:00', 15, 55), (2, '2025-01-10 12:00:00', 19, 50), (2, '2025-01-10 18:00:00', 16, 53), (2, '2025-01-11 08:00:00', 14, 58), (2, '2025-01-11 12:00:00', 20, 48), (3, '2025-01-10 09:00:00', 21, 35), (3, '2025-01-10 14:00:00', 23, 33), (3, '2025-01-11 09:00:00', 22, 34), (3, '2025-01-11 14:00:00', 24, 32);
-- input:
SELECT s.name, DATE_TRUNC('day', r.recorded) AS day, AVG(r.temp_c) AS avg_temp, AVG(r.humidity) AS avg_hum FROM readings r JOIN sensors s ON r.sensor_id = s.id WHERE s.name = 'TH-01' GROUP BY s.name, DATE_TRUNC('day', r.recorded) ORDER BY day;
-- expected output:
TH-01|2025-01-10 00:00:00|20|42.3333
TH-01|2025-01-11 00:00:00|20.3333|42.3333
TH-01|2025-01-12 00:00:00|21.5|40
-- expected status: 0
