package com.pipeline.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * Flink job: Kafka (price-ticks) -> KeyedProcessFunction (MA) -> TimescaleDB.
 */
public class MovingAverageJob {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String kafkaTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "price-ticks");
        String jdbcUrl = System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/pipeline");
        String jdbcUser = System.getenv().getOrDefault("JDBC_USER", "pipeline");
        String jdbcPassword = System.getenv().getOrDefault("JDBC_PASSWORD", "pipeline_secret");

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
        DataStream<PriceTick> ticks = raw
            .filter(s -> s != null && !s.isEmpty())
            .map(MovingAverageJob::parseTick)
            .filter(t -> t != null);

        DataStream<PriceWithMA> withMA = ticks
            .keyBy(PriceTick::getSymbol)
            .process(new MovingAverageFunction());

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

        env.execute("MovingAverageJob");
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
