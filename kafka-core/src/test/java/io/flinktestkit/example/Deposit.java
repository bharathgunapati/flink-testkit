package io.flinktestkit.example;

/** A deposit credited to an account — input for {@link KeyedSumJob}. */
public record Deposit(String accountId, double amount) {
}
