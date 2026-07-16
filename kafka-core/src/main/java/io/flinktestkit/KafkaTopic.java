package io.flinktestkit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares a Kafka topic that a test needs, and the Java type each record
 * on that topic should be (de)serialized as.
 *
 * <p>Annotate a {@code static} field of type {@link TopicHandle} on a test
 * class extended with {@link FlinkTestExtension}. The extension creates a
 * uniquely-named topic on the shared Kafka container, wires up typed
 * producer/consumer support from this single declaration, and injects a
 * ready-to-use {@link TopicHandle} into the field before any {@code @Test}
 * method runs.
 *
 * <p>This one annotation is the single source of truth for the topic's
 * value type — the same type is used to serialize records sent via
 * {@link TopicHandle#produce}, to deserialize records read back via
 * {@link TopicHandle#awaitRecords}, and (via {@link TopicHandle#bootstrapServers()}
 * and {@link TopicHandle#topicName()}) to configure the Flink job under
 * test's own connector. That's what prevents the classic bug where the
 * test producer, the job's source config, and the test's assertion
 * consumer quietly drift out of sync with each other.
 *
 * <pre>{@code
 * @ExtendWith(FlinkTestExtension.class)
 * class MyJobTest {
 *
 *     @KafkaTopic(valueType = Transaction.class)
 *     static TopicHandle<Transaction> input;
 *
 *     @KafkaTopic(valueType = FraudAlert.class)
 *     static TopicHandle<FraudAlert> output;
 *
 *     @Test
 *     void flagsRapidWithdrawals() {
 *         input.produce(new Transaction("acct-6", 500, Instant.now()));
 *         List<FraudAlert> alerts = output.awaitRecords(1, Duration.ofSeconds(10));
 *         assertThat(alerts).extracting(FraudAlert::accountId).containsExactly("acct-6");
 *     }
 * }
 * }</pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaTopic {

    /** The Java type each record's value should be (de)serialized as. */
    Class<?> valueType();

    /** Serialization format for record values ({@link Format#JSON} or {@link Format#AVRO}). */
    Format format() default Format.JSON;

    /** Number of partitions for the generated topic. */
    int partitions() default 1;

    /**
     * Optional name prefix, purely for readability in logs/broker tooling.
     * A random suffix is always appended regardless, so tests stay isolated
     * from each other even when run in parallel.
     */
    String name() default "";

    /**
     * When {@code true}, also create a companion DLQ topic and expose it via
     * {@link TopicHandle#dlq()} / {@link TopicHandle#awaitDlqRecords}. Use
     * this to assert that the job under test routes poison messages to a
     * dead-letter topic instead of crashing or dropping them.
     */
    boolean dlq() default false;
}
