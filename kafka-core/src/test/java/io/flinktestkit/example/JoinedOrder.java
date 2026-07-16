package io.flinktestkit.example;

/** Order enriched with customer name — output of {@link OrderCustomerJoinJob}. */
public record JoinedOrder(String orderId, String customerName, double amount) {
}
