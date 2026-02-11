package com.pipeline.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.OutputTag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Flink job: Kafka (price-ticks) -> KeyedProcessFunction (MA) -> TimescaleDB.
 * Uses event time and watermarks; late-arriving ticks are sent to a side output (log + optional Kafka DLQ).
 */
public class MovingAverageJob {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Side output for ticks that arrive after the watermark has passed their event time + allowed lateness. */
    public static final OutputTag<PriceTick> LATE_TICKS = new OutputTag<PriceTick>("late-ticks") {};

    /** Default: events can be at most this many seconds behind the max event time (watermark bound). */
    private static final int DEFAULT_WATERMARK_OUT_OF_ORDER_SEC = 10;
    /** Default: events with eventTime < watermark - this are considered late. */
    private static final int DEFAULT_ALLOWED_LATENESS_SEC = 5;

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String kafkaTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "price-ticks");
        String jdbcUrl = System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/pipeline");
        String jdbcUser = System.getenv().getOrDefault("JDBC_USER", "pipeline");
        String jdbcPassword = System.getenv().getOrDefault("JDBC_PASSWORD", "pipeline_secret");

        int outOfOrderSec = parseIntEnv("WATERMARK_OUT_OF_ORDER_SEC", DEFAULT_WATERMARK_OUT_OF_ORDER_SEC);
        int allowedLatenessSec = parseIntEnv("ALLOWED_LATENESS_SEC", DEFAULT_ALLOWED_LATENESS_SEC);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId("moving-average-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> raw = env.fromSource(kafkaSource, WatermarkStrategy.<String>noWatermarks(), "Kafka Source");
        DataStream<PriceTick> parsed = raw
            .filter(s -> s != null && !s.isEmpty())
            .map(MovingAverageJob::parseTick)
            .filter(t -> t != null);

        // Only ticks with valid timestamp get event time and watermarks; skip invalid to avoid fake time
        DataStream<PriceTick> ticks = parsed
            .filter(t -> hasValidTimestamp(t.getTimestamp()))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<PriceTick>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderSec))
                    .withTimestampAssigner((tick, prev) -> parseTimestampToMillis(tick.getTimestamp()))
            );

        long allowedLatenessMs = allowedLatenessSec * 1000L;
        SingleOutputStreamOperator<PriceWithMA> withMA = ticks
            .keyBy(PriceTick::getSymbol)
            .process(new MovingAverageFunction(allowedLatenessMs, LATE_TICKS));

        JdbcExecutionOptions execOpts = JdbcExecutionOptions.builder()
            .withBatchSize(100)
            .withBatchIntervalMs(200)
            .build();
        JdbcConnectionOptions connOpts = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(jdbcUrl)
            .withUsername(jdbcUser)
            .withPassword(jdbcPassword)
            .build();

        withMA.addSink(JdbcSink.sink(
            "INSERT INTO price_ticks (time, symbol, price, volume, ma_short, ma_long) VALUES (?, ?, ?, ?, ?, ?)",
            (PreparedStatement ps, PriceWithMA row) -> {
                ps.setTimestamp(1, Timestamp.from(row.getTime()));
                ps.setString(2, row.getSymbol());
                ps.setDouble(3, row.getPrice());
                ps.setDouble(4, row.getVolume());
                ps.setObject(5, row.getMaShort());
                ps.setObject(6, row.getMaLong());
            },
            execOpts,
            connOpts
        ));

        // Late-arriving ticks: always log; optionally sink to Kafka DLQ
        DataStream<PriceTick> lateTicks = withMA.getSideOutput(LATE_TICKS);
        lateTicks.addSink(new SinkFunction<PriceTick>() {
            @Override
            public void invoke(PriceTick value, SinkFunction.Context context) {
                System.out.println("[LATE] " + value.getSymbol() + " @ " + value.getTimestamp() + " price=" + value.getPrice());
            }
        });

        String lateTopic = System.getenv().get("LATE_TICKS_KAFKA_TOPIC");
        if (lateTopic != null && !lateTopic.isEmpty()) {
            lateTicks
                .map(tick -> {
                    try {
                        return JSON.writeValueAsString(tick);
                    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .sinkTo(org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                    .setBootstrapServers(kafkaBootstrap)
                    .setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                        .setTopic(lateTopic)
                        .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                        .build())
                    .build());
        }

        env.execute("MovingAverageJob");
    }

    private static int parseIntEnv(String key, int defaultVal) {
        String v = System.getenv().get(key);
        if (v == null || v.isEmpty()) return defaultVal;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    private static boolean hasValidTimestamp(String ts) {
        if (ts == null || ts.isEmpty()) return false;
        try {
            Instant.parse(ts);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static long parseTimestampToMillis(String ts) {
        if (ts == null || ts.isEmpty()) return Long.MIN_VALUE;
        try {
            return Instant.parse(ts).toEpochMilli();
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    private static PriceTick parseTick(String json) {
        try {
            JsonNode n = JSON.readTree(json);
            PriceTick t = new PriceTick();
            t.setSymbol(n.has("symbol") ? n.get("symbol").asText() : "");
            t.setTimestamp(n.has("timestamp") ? n.get("timestamp").asText() : null);
            t.setPrice(n.has("price") ? n.get("price").asDouble() : 0);
            t.setVolume(n.has("volume") ? n.get("volume").asDouble() : 0);
            return t;
        } catch (Exception e) {
            return null;
        }
    }
}
