package io.flinktestkit.example;

/** Click with event-time timestamp — input for {@link WindowCountJob}. */
public record ClickEvent(String userId, long eventTimeMillis) {
}
