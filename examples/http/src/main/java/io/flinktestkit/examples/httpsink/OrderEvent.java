package io.flinktestkit.examples.httpsink;

/** Sample domain type for the HTTP sink consumer example. */
public record OrderEvent(String orderId, String customerName, double amount) {
}
