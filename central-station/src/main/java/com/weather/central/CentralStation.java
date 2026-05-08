package com.weather.central;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class CentralStation {

    private static final Logger log = LoggerFactory.getLogger(CentralStation.class);

    private static final String TOPIC = "weather-readings";
    private static final int BATCH_SIZE = 5_000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final Duration MAX_BUFFER_AGE = Duration.ofMinutes(5);

    public static void main(String[] args) {

        String bootstrap = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
        String jdbcUrl = System.getenv()
                .getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/weather_db");
        String dbUser = System.getenv().getOrDefault("DB_USER", "weather_user");
        String dbPass = System.getenv().getOrDefault("DB_PASSWORD", "weather_pass");

        log.info("Central Station starting. Kafka={}, DB={}", bootstrap, jdbcUrl);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "central-station");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // We commit manually after a successful DB write — no auto-commit.
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Tune fetch size to bring back enough messages to fill batches quickly
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        ObjectMapper mapper = new ObjectMapper();

        try (WeatherReadingDao dao = new WeatherReadingDao(jdbcUrl, dbUser, dbPass);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(TOPIC));

            // Graceful shutdown
            Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal — waking consumer.");
                consumer.wakeup();
                try { mainThread.join(); } catch (InterruptedException ignored) {}
            }));

            List<WeatherReadingDto> buffer = new ArrayList<>(BATCH_SIZE);
            long bufferOpenedAt = System.nanoTime();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

                if (records.isEmpty() && buffer.isEmpty()) {
                    bufferOpenedAt = System.nanoTime();  // reset age timer
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    if (buffer.isEmpty()) {
                        bufferOpenedAt = System.nanoTime();
                    }
                    WeatherReadingDto dto = parse(mapper, record.value());
                    if (dto != null) {
                        buffer.add(dto);
                    }
                }

                boolean batchFull = buffer.size() >= BATCH_SIZE;
                boolean batchStale = !buffer.isEmpty()
                        && (System.nanoTime() - bufferOpenedAt) > MAX_BUFFER_AGE.toNanos();

                if (batchFull || batchStale) {
                    try {
                        dao.insertBatch(buffer);
                        consumer.commitSync();   // commit offsets ONLY after DB succeeded
                        buffer.clear();
                        bufferOpenedAt = System.nanoTime();
                    } catch (SQLException e) {
                        log.error("Batch insert failed; will retry on next poll.", e);
                        // Don't commit. Don't clear buffer. Next loop will try again.
                    }
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer woken up — exiting.");
        } catch (SQLException e) {
            log.error("Fatal DB error", e);
            System.exit(1);
        }
    }

    private static WeatherReadingDto parse(ObjectMapper mapper, String json) {
        try {
            JsonNode r = mapper.readTree(json);
            JsonNode w = r.path("weather");
            return new WeatherReadingDto(
                    r.path("station_id").asLong(),
                    r.path("s_no").asLong(),
                    r.path("battery_status").asText(),
                    r.path("status_timestamp").asLong(),
                    w.path("humidity").asInt(),
                    w.path("temperature").asInt(),
                    w.path("wind_speed").asInt()
            );
        } catch (Exception e) {
            log.warn("Skipping malformed message: {}", json);
            return null;
        }
    }
}