package com.weather.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RainProcessor {

    private static final Logger log = LoggerFactory.getLogger(RainProcessor.class);

    private static final String INPUT_TOPIC  = "weather-readings";
    private static final String OUTPUT_TOPIC = "rain-alerts";
    private static final int    HUMIDITY_THRESHOLD = 70;

    public static void main(String[] args) {

        String bootstrap = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");

        log.info("Starting Rain Processor -> Kafka {}", bootstrap);

        // 1) Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // For dev: start from earliest offsets so we see existing messages on first run
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        // 2) Build the topology: read -> filter -> write
        ObjectMapper mapper = new ObjectMapper();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> {
                    int humidity = parseHumidity(mapper, value);
                    boolean isRain = humidity > HUMIDITY_THRESHOLD;
                    if (isRain) {
                        log.info("RAIN ALERT: station={} humidity={}", key, humidity);
                    }
                    return isRain;
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 3) Graceful shutdown
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping streams.");
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            log.info("Rain Processor running. Filtering humidity > {}.", HUMIDITY_THRESHOLD);
            latch.await();
        } catch (Throwable e) {
            log.error("Streams crashed", e);
            System.exit(1);
        }
    }

    /** Extract the humidity from the nested "weather" object. Returns -1 on parse failure. */
    private static int parseHumidity(ObjectMapper mapper, String json) {
        try {
            JsonNode root = mapper.readTree(json);
            JsonNode weather = root.path("weather");
            return weather.path("humidity").asInt(-1);
        } catch (Exception e) {
            log.warn("Failed to parse humidity from message: {}", json);
            return -1;
        }
    }
}