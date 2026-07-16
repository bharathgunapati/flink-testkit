package io.flinktestkit;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Field;

/**
 * JUnit 5 extension that composes connector harnesses for Flink integration
 * tests. Connector modules plug in via SPI:
 *
 * <ul>
 *   <li>{@code flink-testkit-kafka-core} — {@code @KafkaTopic} / {@link KafkaTopicPlugin}</li>
 *   <li>{@code flink-testkit-jdbc} — {@code @JdbcTable} / {@link JdbcTablePlugin}</li>
 *   <li>{@code flink-testkit-http} — {@code @HttpEndpoint} / {@link HttpEndpointPlugin}</li>
 * </ul>
 *
 * <p>Containers start lazily inside each plugin on first use. This module
 * stays free of Kafka, JDBC, HTTP-mock, and Testcontainers dependencies.
 */
public final class FlinkTestExtension implements BeforeAllCallback, AfterAllCallback {

    private static final String KAFKA_TOPIC = "io.flinktestkit.KafkaTopic";
    private static final String JDBC_TABLE = "io.flinktestkit.JdbcTable";
    private static final String HTTP_ENDPOINT = "io.flinktestkit.HttpEndpoint";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        if (hasAnnotation(testClass, KAFKA_TOPIC)) {
            KafkaTopicPlugins.require().beforeAll(testClass);
        }
        if (hasAnnotation(testClass, JDBC_TABLE)) {
            JdbcTablePlugins.require().beforeAll(testClass);
        }
        if (hasAnnotation(testClass, HTTP_ENDPOINT)) {
            HttpEndpointPlugins.require().beforeAll(testClass);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();
        Exception first = null;
        first = runAfter(first, testClass, KAFKA_TOPIC, () -> KafkaTopicPlugins.require().afterAll(testClass));
        first = runAfter(first, testClass, JDBC_TABLE, () -> JdbcTablePlugins.require().afterAll(testClass));
        first = runAfter(first, testClass, HTTP_ENDPOINT, () -> HttpEndpointPlugins.require().afterAll(testClass));
        if (first != null) {
            throw first;
        }
    }

    private static Exception runAfter(
            Exception first,
            Class<?> testClass,
            String annotationTypeName,
            AfterAction action) {
        if (!hasAnnotation(testClass, annotationTypeName)) {
            return first;
        }
        try {
            action.run();
            return first;
        } catch (Exception e) {
            if (first == null) {
                return e;
            }
            first.addSuppressed(e);
            return first;
        }
    }

    @FunctionalInterface
    private interface AfterAction {
        void run() throws Exception;
    }

    private static boolean hasAnnotation(Class<?> testClass, String annotationTypeName) {
        for (Field field : testClass.getDeclaredFields()) {
            for (var annotation : field.getAnnotations()) {
                if (annotationTypeName.equals(annotation.annotationType().getName())) {
                    return true;
                }
            }
        }
        return false;
    }
}
