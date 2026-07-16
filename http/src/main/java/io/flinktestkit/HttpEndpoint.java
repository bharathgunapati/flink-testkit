package io.flinktestkit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares an HTTP endpoint that a test needs (backed by a shared MockServer
 * container).
 *
 * <p>Annotate a {@code static} field of type {@link HttpHandle} on a test
 * class extended with {@link FlinkTestExtension}. Requires
 * {@code flink-testkit-http} on the test classpath.
 *
 * <pre>{@code
 * @ExtendWith(FlinkTestExtension.class)
 * class MyJobTest {
 *
 *     @HttpEndpoint(path = "/ingest")
 *     static HttpHandle ingest;
 *
 *     @Test
 *     void postsEvents() {
 *         ingest.stubPost(202);
 *         // job uses ingest.url()
 *         List<ReceivedRequest> hits = ingest.awaitRequests(1, Duration.ofSeconds(10));
 *     }
 * }
 * }</pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HttpEndpoint {

    /**
     * Path template for this endpoint (must start with {@code /}).
     * A unique suffix is appended so tests stay isolated on the shared server.
     */
    String path();

    /**
     * Optional name prefix for readability in logs. A random suffix is always
     * appended to the path regardless.
     */
    String name() default "";
}
