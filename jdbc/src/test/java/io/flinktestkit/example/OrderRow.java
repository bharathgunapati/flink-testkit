package io.flinktestkit.example;

/**
 * Example row type for {@link OrderJdbcSinkJob} / {@link OrderJdbcSinkJobTest}.
 * Not part of the flink-testkit library — fixture data for the demo.
 */
public record OrderRow(String orderId, String customerName, double amount) {
}
