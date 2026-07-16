package io.flinktestkit.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * Stateful keyed aggregation: running sum of {@link Deposit}s per accountId,
 * emitting an {@link AccountTotal} after every deposit. Output records are
 * keyed by accountId so tests can use {@code awaitRecordsByKey}.
 */
public final class KeyedSumJob {

    private KeyedSumJob() {
    }

    public static JobClient runAsync(String bootstrapServers, String inputTopic, String outputTopic)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("keyed-sum-job-" + System.nanoTime())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        SerializationSchema<String> accountIdKeySchema = json -> {
            AccountTotal total = ExampleJson.read(json, AccountTotal.class);
            return total.accountId().getBytes(StandardCharsets.UTF_8);
        };

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setKeySerializationSchema(accountIdKeySchema)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        DataStream<String> input = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "deposits");

        input
            .map(json -> ExampleJson.read(json, Deposit.class))
            .returns(Deposit.class)
            .keyBy(Deposit::accountId)
            .map(new RunningTotal())
            .map(ExampleJson::write)
            .sinkTo(sink);

        return env.executeAsync("keyed-sum-job");
    }

    private static final class RunningTotal extends RichMapFunction<Deposit, AccountTotal> {
        private transient ValueState<Double> total;

        @Override
        public void open(Configuration parameters) {
            total = getRuntimeContext().getState(
                new ValueStateDescriptor<>("total", Types.DOUBLE));
        }

        @Override
        public AccountTotal map(Deposit deposit) throws Exception {
            Double current = total.value();
            double next = (current == null ? 0.0 : current) + deposit.amount();
            total.update(next);
            return new AccountTotal(deposit.accountId(), next);
        }
    }
}
