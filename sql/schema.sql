-- Drop and recreate for a clean slate (safe in dev; remove in production)
DROP TABLE IF EXISTS weather_readings;

CREATE TABLE weather_readings (
    id              BIGSERIAL PRIMARY KEY,
    station_id      BIGINT      NOT NULL,
    sequence_number BIGINT      NOT NULL,
    battery_status  VARCHAR(10) NOT NULL,
    timestamp       BIGINT      NOT NULL,
    humidity        INT         NOT NULL,
    temperature     INT         NOT NULL,
    wind_speed      INT         NOT NULL,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Helpful indexes for the analytical queries
CREATE INDEX idx_weather_station_id  ON weather_readings(station_id);
CREATE INDEX idx_weather_battery     ON weather_readings(battery_status);