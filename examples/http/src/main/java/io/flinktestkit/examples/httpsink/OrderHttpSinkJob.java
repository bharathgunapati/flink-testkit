package io.flinktestkit.examples.httpsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

/**
 * Sample Flink job using Apache {@code flink-connector-http} HttpSink.
 * Consumer-style example under {@code examples/http}, not part of the library.
 */
public final class OrderHttpSinkJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OrderHttpSinkJob() {
    }

    public static JobClient runAsync(String endpointUrl, OrderEvent... events) throws Exception {
        String[] jsonBodies = new String[events.length];
        for (int i = 0; i < events.length; i++) {
            jsonBodies[i] = MAPPER.writeValueAsString(events[i]);
        }
        return runAsyncJson(endpointUrl, jsonBodies);
    }

    public static JobClient runAsyncJson(String endpointUrl, String... jsonBodies) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.fromElements(jsonBodies);

        HttpSink<String> sink = HttpSink.<String>builder()
            .setEndpointUrl(endpointUrl)
            .setElementConverter((json, context) -> new HttpSinkRequestEntry(
                "POST",
                json.getBytes(StandardCharsets.UTF_8)))
            .setProperty("http.sink.writer.request.mode", "single")
            .setProperty("http.sink.header.Content-Type", "application/json")
            .build();

        stream.sinkTo(sink);
        return env.executeAsync("examples-order-http-sink");
    }
}
