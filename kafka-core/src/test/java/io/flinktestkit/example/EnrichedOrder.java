package io.flinktestkit.example;

/**
 * Example output type for {@link UppercaseJob} / {@link UppercaseJobTest}.
 * Not part of the flink-testkit library — just fixture data for the demo.
 */
public record EnrichedOrder(String orderId, String customerName, double amount) {
}
