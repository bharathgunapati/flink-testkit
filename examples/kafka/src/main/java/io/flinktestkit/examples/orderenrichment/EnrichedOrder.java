package io.flinktestkit.examples.orderenrichment;

/** Sample output type for the consumer-style example job. */
public record EnrichedOrder(String orderId, String customerName, double amount) {
}
