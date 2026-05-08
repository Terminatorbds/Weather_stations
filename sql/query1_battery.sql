-- Query 1: Battery distribution per station
-- Expected: ~30% low / ~40% medium / ~30% high per station
SELECT station_id,
       battery_status,
       COUNT(*) AS total,
       ROUND(100.0 * COUNT(*) /
             SUM(COUNT(*)) OVER (PARTITION BY station_id), 1) AS pct
FROM weather_readings
GROUP BY station_id, battery_status
ORDER BY station_id, battery_status;