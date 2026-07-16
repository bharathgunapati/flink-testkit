package io.flinktestkit.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Multi-source join: {@link Order}s enriched with {@link Customer} name
 * keyed by customerId. Customers may arrive before or after orders;
 * unmatched orders wait in state until the customer profile shows up.
 */
public final class OrderCustomerJoinJob {

    private OrderCustomerJoinJob() {
    }

    public static JobClient runAsync(
            String bootstrapServers,
            String ordersTopic,
            String customersTopic,
            String outputTopic) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> ordersSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(ordersTopic)
            .setGroupId("join-orders-" + System.nanoTime())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> customersSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(customersTopic)
            .setGroupId("join-customers-" + System.nanoTime())
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

        DataStream<Order> orders = env
            .fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "orders")
            .map(json -> ExampleJson.read(json, Order.class))
            .returns(Order.class);

        DataStream<Customer> customers = env
            .fromSource(customersSource, WatermarkStrategy.noWatermarks(), "customers")
            .map(json -> ExampleJson.read(json, Customer.class))
            .returns(Customer.class);

        orders
            .keyBy(Order::customerId)
            .connect(customers.keyBy(Customer::customerId))
            .flatMap(new JoinOrdersToCustomers())
            .map(ExampleJson::write)
            .sinkTo(sink);

        return env.executeAsync("order-customer-join-job");
    }

    private static final class JoinOrdersToCustomers
            extends RichCoFlatMapFunction<Order, Customer, JoinedOrder> {

        private transient ValueState<String> customerName;
        private transient ValueState<Order> pendingOrder;

        @Override
        public void open(Configuration parameters) {
            customerName = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customerName", Types.STRING));
            pendingOrder = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pendingOrder", TypeInformation.of(Order.class)));
        }

        @Override
        public void flatMap1(Order order, Collector<JoinedOrder> out) throws Exception {
            String name = customerName.value();
            if (name != null) {
                out.collect(new JoinedOrder(order.orderId(), name, order.amount()));
            } else {
                pendingOrder.update(order);
            }
        }

        @Override
        public void flatMap2(Customer customer, Collector<JoinedOrder> out) throws Exception {
            customerName.update(customer.name());
            Order pending = pendingOrder.value();
            if (pending != null) {
                out.collect(new JoinedOrder(pending.orderId(), customer.name(), pending.amount()));
                pendingOrder.clear();
            }
        }
    }
}
