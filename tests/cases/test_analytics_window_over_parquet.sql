-- analytics stress: window RANK over parquet table with WHERE filter
-- setup:
CREATE FOREIGN TABLE awp_events OPTIONS (FILENAME '@@FIXTURES@@/mini_events.parquet');
-- input:
SELECT event_type, user_id, score, RANK() OVER (PARTITION BY event_type ORDER BY score DESC) FROM awp_events WHERE score > 8000 ORDER BY event_type, score DESC;
-- expected output:
0|1|9500|1
0|0|9100|2
0|0|9000|3
0|0|8500|4
0|2|8200|5
-- expected status: 0
