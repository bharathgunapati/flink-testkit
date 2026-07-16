package io.flinktestkit.examples.jdbcsink;

/** Sample domain type for the Kafka → JDBC consumer example. */
public record OrderRow(String orderId, String customerName, double amount) {
}
