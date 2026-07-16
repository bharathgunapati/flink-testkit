package io.flinktestkit.example;

/**
 * Example event type for {@link OrderHttpSinkJob}. Fixture only.
 */
public record OrderEvent(String orderId, String customerName, double amount) {
}
