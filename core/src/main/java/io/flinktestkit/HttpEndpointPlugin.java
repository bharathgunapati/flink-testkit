package io.flinktestkit;

/**
 * Optional SPI implemented by {@code flink-testkit-http}.
 * Core stays free of MockServer/Testcontainers dependencies; {@link FlinkTestExtension}
 * loads this via {@link java.util.ServiceLoader} when present.
 */
public interface HttpEndpointPlugin {

    /**
     * Scans {@code testClass} for {@code @HttpEndpoint} fields, starts MockServer
     * lazily if needed, stubs endpoints, and injects typed handles.
     */
    void beforeAll(Class<?> testClass) throws Exception;

    /**
     * Resets stubs/recorded requests created for {@code testClass}.
     */
    void afterAll(Class<?> testClass) throws Exception;
}
