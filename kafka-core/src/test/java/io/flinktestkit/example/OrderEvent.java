package io.flinktestkit.example;

/**
 * Example input type for {@link UppercaseJob} / {@link UppercaseJobTest}.
 * Not part of the flink-testkit library — just fixture data for the demo.
 */
public record OrderEvent(String orderId, String customerName, double amount) {
}
