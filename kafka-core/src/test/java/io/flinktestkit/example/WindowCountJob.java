package io.flinktestkit.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Event-time tumbling window count of {@link ClickEvent}s per user
 * (5-second windows). Demonstrates windowed Flink + Kafka integration
 * testing with flink-testkit.
 */
public final class WindowCountJob {

    private WindowCountJob() {
    }

    public static JobClient runAsync(String bootstrapServers, String inputTopic, String outputTopic)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(inputTopic)
            .setGroupId("window-count-job-" + System.nanoTime())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(outputTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        WatermarkStrategy<ClickEvent> watermarks = WatermarkStrategy
            .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((event, unused) -> event.eventTimeMillis());

        DataStream<String> raw = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "clicks-raw");

        raw
            .map(json -> ExampleJson.read(json, ClickEvent.class))
            .returns(ClickEvent.class)
            .assignTimestampsAndWatermarks(watermarks)
            .keyBy(ClickEvent::userId)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .aggregate(new CountAggregate(), new WindowCountProcess())
            .map(ExampleJson::write)
            .sinkTo(sink);

        return env.executeAsync("window-count-job");
    }

    private static final class CountAggregate implements AggregateFunction<ClickEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ClickEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static final class WindowCountProcess
            extends ProcessWindowFunction<Long, WindowCount, String, TimeWindow> {

        @Override
        public void process(
                String userId,
                Context context,
                Iterable<Long> counts,
                Collector<WindowCount> out) {
            long count = counts.iterator().next();
            out.collect(new WindowCount(userId, context.window().getStart(), count));
        }
    }
}
