package io.flinktestkit.example;

/** Order referencing a customer — input for {@link OrderCustomerJoinJob}. */
public record Order(String orderId, String customerId, double amount) {
}
