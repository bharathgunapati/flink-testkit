package io.flinktestkit.example;

/** Per-user count for one tumbling window — output of {@link WindowCountJob}. */
public record WindowCount(String userId, long windowStartMillis, long count) {
}
