package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(FlinkTestExtension.class)
class OrderCustomerJoinJobTest {

    @KafkaTopic(valueType = Order.class)
    static TopicHandle<Order> orders;

    @KafkaTopic(valueType = Customer.class)
    static TopicHandle<Customer> customers;

    @KafkaTopic(valueType = JoinedOrder.class)
    static TopicHandle<JoinedOrder> joined;

    @Test
    void enrichesOrderWhenCustomerArrivesFirst() throws Exception {
        JobClient job = OrderCustomerJoinJob.runAsync(
            orders.bootstrapServers(),
            orders.topicName(),
            customers.topicName(),
            joined.topicName());

        try {
            customers.produce(new Customer("c-1", "Ada"));
            orders.produce(new Order("o-1", "c-1", 42.0));

            List<JoinedOrder> results = joined.awaitRecords(
                records -> records.stream().anyMatch(r -> "o-1".equals(r.orderId())),
                Duration.ofSeconds(45));

            assertThat(results)
                .filteredOn(r -> "o-1".equals(r.orderId()))
                .first()
                .extracting(JoinedOrder::customerName, JoinedOrder::amount)
                .containsExactly("Ada", 42.0);
        } finally {
            job.cancel();
        }
    }

    @Test
    void enrichesOrderWhenOrderArrivesBeforeCustomer() throws Exception {
        JobClient job = OrderCustomerJoinJob.runAsync(
            orders.bootstrapServers(),
            orders.topicName(),
            customers.topicName(),
            joined.topicName());

        try {
            orders.produce(new Order("o-2", "c-2", 7.5));
            customers.produce(new Customer("c-2", "Grace"));

            List<JoinedOrder> results = joined.awaitRecords(
                records -> records.stream().anyMatch(r -> "o-2".equals(r.orderId())),
                Duration.ofSeconds(45));

            assertThat(results)
                .filteredOn(r -> "o-2".equals(r.orderId()))
                .first()
                .extracting(JoinedOrder::customerName)
                .isEqualTo("Grace");
        } finally {
            job.cancel();
        }
    }
}
