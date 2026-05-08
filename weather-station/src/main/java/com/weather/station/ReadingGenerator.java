package com.weather.station;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random weather readings.
 * Uses ThreadLocalRandom for good randomness without contention.
 */
public class ReadingGenerator {

    private final long stationId;

    public ReadingGenerator(long stationId) {
        this.stationId = stationId;
    }

    public WeatherReading generate(long sNo) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        return new WeatherReading(
                stationId,
                sNo,
                pickBatteryStatus(rnd),
                Instant.now().getEpochSecond(),
                rnd.nextInt(0, 101),     // humidity 0..100 %
                rnd.nextInt(-20, 121),   // temperature -20..120 °F
                rnd.nextInt(0, 151)      // wind speed 0..150 km/h
        );
    }

    /**
     * Weighted random pick: 30% low, 40% medium, 30% high.
     *
     * Roll a number r in [0, 100):
     *   r < 30           -> low      (probability 0.30)
     *   30 <= r < 70     -> medium   (probability 0.40)
     *   r >= 70          -> high     (probability 0.30)
     */
    private String pickBatteryStatus(ThreadLocalRandom rnd) {
        int r = rnd.nextInt(100);
        if (r < 30) return "low";
        if (r < 70) return "medium";
        return "high";
    }
}