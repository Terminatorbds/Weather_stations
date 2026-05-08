package com.weather.station;

/**
 * One reading from a weather station. Immutable.
 *
 * Java records auto-generate constructor, getters, equals(), hashCode(), toString().
 * The JSON shape is built in WeatherStation.toJson() — we don't rely on
 * Jackson's auto-mapping here because the spec wants a nested "weather" object
 * that doesn't match a flat record's structure.
 */
public record WeatherReading(
        long stationId,
        long sNo,
        String batteryStatus,
        long statusTimestamp,
        int humidity,
        int temperature,
        int windSpeed
) {}