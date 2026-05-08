-- Query 2: Estimated dropped messages per station
--
-- IMPORTANT INTERPRETATION:
-- Per the project spec, s_no is incremented ONLY on actual sends, not on drops.
-- Therefore:
--   MAX(sequence_number) = messages this station SUCCESSFULLY SENT to Kafka
--   COUNT(*)             = messages the database has RECEIVED and stored
--   estimated_dropped    = post-Kafka loss (consumer issues, DB issues)
--
-- The deliberate 10% producer drop rate is NOT measurable from this query
-- alone — those messages were never sent in the first place, so s_no never
-- counted them. In a healthy steady-state run, estimated_dropped is near zero.
SELECT station_id,
       MAX(sequence_number) AS highest_seq,
       COUNT(*) AS received,
       MAX(sequence_number) - COUNT(*) AS estimated_dropped
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;