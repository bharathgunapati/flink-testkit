package io.flinktestkit.example;

/** Customer profile — second input for {@link OrderCustomerJoinJob}. */
public record Customer(String customerId, String name) {
}
