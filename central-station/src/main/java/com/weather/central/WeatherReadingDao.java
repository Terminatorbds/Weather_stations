package com.weather.central;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Handles the JDBC connection and batch INSERT of weather readings.
 * Holds a single long-lived connection (one consumer thread, single-threaded use).
 */
public class WeatherReadingDao implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WeatherReadingDao.class);

    private static final String INSERT_SQL = """
            INSERT INTO weather_readings
              (station_id, sequence_number, battery_status, timestamp,
               humidity, temperature, wind_speed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final Connection connection;

    public WeatherReadingDao(String jdbcUrl, String user, String password) throws SQLException {
        log.info("Connecting to Postgres: {}", jdbcUrl);
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        // Use explicit transactions (default is autocommit=true, which we don't want for batches)
        this.connection.setAutoCommit(false);
    }

    /**
     * Inserts the given list of readings as ONE batch in ONE transaction.
     * Either all of them get inserted or none — atomicity matters for crash safety.
     */
    public void insertBatch(List<WeatherReadingDto> batch) throws SQLException {
        if (batch.isEmpty()) return;

        long start = System.nanoTime();

        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            for (WeatherReadingDto r : batch) {
                ps.setLong(1, r.stationId());
                ps.setLong(2, r.sequenceNumber());
                ps.setString(3, r.batteryStatus());
                ps.setLong(4, r.timestamp());
                ps.setInt(5, r.humidity());
                ps.setInt(6, r.temperature());
                ps.setInt(7, r.windSpeed());
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();

            long ms = (System.nanoTime() - start) / 1_000_000;
            log.info("Inserted batch of {} rows in {} ms ({} rows/sec)",
                    batch.size(), ms, ms == 0 ? "∞" : (batch.size() * 1000L / ms));
        } catch (SQLException e) {
            log.error("Batch insert failed — rolling back", e);
            connection.rollback();
            throw e;
        }
    }

    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}