package io.flinktestkit.examples.orderenrichment;

/** Sample domain type for the consumer-style example job. */
public record OrderEvent(String orderId, String customerName, double amount) {
}
