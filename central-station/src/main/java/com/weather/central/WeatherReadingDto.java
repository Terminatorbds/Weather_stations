package com.weather.central;

public record WeatherReadingDto(
        long stationId,
        long sequenceNumber,
        String batteryStatus,
        long timestamp,
        int humidity,
        int temperature,
        int windSpeed
) {}