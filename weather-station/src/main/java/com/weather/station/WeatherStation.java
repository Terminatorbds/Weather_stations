package com.weather.station;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class WeatherStation {

    private static final Logger log = LoggerFactory.getLogger(WeatherStation.class);

    private static final String TOPIC = "weather-readings";
    private static final double DROP_PROBABILITY = 0.10;
    private static final long EMIT_INTERVAL_MS = 1000;

    public static void main(String[] args) throws InterruptedException {

        // 1) Read config from environment variables (with sane defaults)
        long stationId = parseStationId(
                System.getenv().getOrDefault("STATION_ID", "1"));
        String bootstrap = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");

        log.info("Starting Weather Station {} -> Kafka {}", stationId, bootstrap);

        // 2) Configure the Kafka producer
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "station-" + stationId);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");

        ObjectMapper mapper = new ObjectMapper();
        ReadingGenerator generator = new ReadingGenerator(stationId);
        long sNo = 0;

        // 3) Producer is AutoCloseable -> try-with-resources flushes & closes on exit
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Graceful shutdown on Ctrl+C / SIGTERM
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
                    log.info("Shutdown signal received — closing producer.")));

            while (true) {
                // 10% drop — and per spec, sNo only advances when we actually send
                if (ThreadLocalRandom.current().nextDouble() < DROP_PROBABILITY) {
                    log.debug("Station {} dropped a message (s_no NOT incremented)", stationId);
                } else {
                    sNo++;
                    WeatherReading reading = generator.generate(sNo);
                    String json = toJson(mapper, reading);

                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            TOPIC,
                            String.valueOf(stationId),  // key = station id (groups partitions)
                            json
                    );

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Send failed for station {} s_no {}: {}",
                                    stationId, reading.sNo(), exception.getMessage());
                        } else {
                            log.info("Sent: station={} s_no={} battery={} humidity={} -> partition={} offset={}",
                                    reading.stationId(), reading.sNo(),
                                    reading.batteryStatus(), reading.humidity(),
                                    metadata.partition(), metadata.offset());
                        }
                    });
                }

                Thread.sleep(EMIT_INTERVAL_MS);
            }
        }
    }

    /**
     * Turn the STATION_ID env var into a Long.
     * - Plain number ("1", "7"): used as-is. (Local dev.)
     * - StatefulSet pod name ("weather-station-3"): take the part after the last dash, add 1
     *   so we get 1..10 instead of 0..9.
     */
    private static long parseStationId(String raw) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {}

        return Long.parseLong(raw.substring(raw.lastIndexOf('-') + 1)) + 1;
    }

    /**
     * Build the exact JSON shape the spec requires:
     * { station_id, s_no, battery_status, status_timestamp, weather: { humidity, temperature, wind_speed } }
     */
    private static String toJson(ObjectMapper mapper, WeatherReading r) {
        ObjectNode root = mapper.createObjectNode();
        root.put("station_id", r.stationId());
        root.put("s_no", r.sNo());
        root.put("battery_status", r.batteryStatus());
        root.put("status_timestamp", r.statusTimestamp());

        ObjectNode weather = root.putObject("weather");
        weather.put("humidity", r.humidity());
        weather.put("temperature", r.temperature());
        weather.put("wind_speed", r.windSpeed());

        return root.toString();
    }
}