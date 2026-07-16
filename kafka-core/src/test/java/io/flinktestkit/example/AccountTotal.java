package io.flinktestkit.example;

/** Running total per account — output of {@link KeyedSumJob}. */
public record AccountTotal(String accountId, double total) {
}
