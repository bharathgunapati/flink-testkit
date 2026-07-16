package io.flinktestkit;

import org.mockserver.client.MockServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * SPI implementation: shared MockServer lifecycle + {@link HttpEndpoint} injection.
 */
public final class HttpEndpointPluginImpl implements HttpEndpointPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(HttpEndpointPluginImpl.class);
    private static final DockerImageName IMAGE =
        DockerImageName.parse("mockserver/mockserver:5.15.0");

    private static final Object LOCK = new Object();
    private static volatile MockServerContainer mockServer;
    private static volatile MockServerClient sharedClient;

    private final ThreadLocal<List<HttpHandle>> handlesForCurrentClass =
        ThreadLocal.withInitial(ArrayList::new);

    @Override
    public void beforeAll(Class<?> testClass) throws Exception {
        List<HttpHandle> created = new ArrayList<>();

        for (Field field : testClass.getDeclaredFields()) {
            HttpEndpoint annotation = field.getAnnotation(HttpEndpoint.class);
            if (annotation == null) {
                continue;
            }
            validateField(field);

            HttpHandle handle = createHandle(field, annotation);
            created.add(handle);

            field.setAccessible(true);
            field.set(null, handle);
        }

        handlesForCurrentClass.set(created);
    }

    @Override
    public void afterAll(Class<?> testClass) throws Exception {
        for (HttpHandle handle : handlesForCurrentClass.get()) {
            try {
                handle.clear();
            } catch (Exception e) {
                LOG.debug("Could not clear MockServer path '{}': {}", handle.path(), e.toString());
            }
        }
        handlesForCurrentClass.remove();
    }

    private void validateField(Field field) {
        if (!Modifier.isStatic(field.getModifiers())) {
            throw new IllegalStateException(
                "@HttpEndpoint field '" + field.getName() + "' on " + field.getDeclaringClass().getSimpleName()
                    + " must be static, so it can be populated in beforeAll() and shared "
                    + "across every @Test method in the class.");
        }
        if (!HttpHandle.class.equals(field.getType())) {
            throw new IllegalStateException(
                "@HttpEndpoint field '" + field.getName() + "' on " + field.getDeclaringClass().getSimpleName()
                    + " must be of type HttpHandle.");
        }
    }

    private HttpHandle createHandle(Field field, HttpEndpoint annotation) {
        ensureMockServer();

        String rawPath = annotation.path();
        if (rawPath == null || rawPath.isBlank() || !rawPath.startsWith("/")) {
            throw new IllegalStateException(
                "@HttpEndpoint field '" + field.getName() + "' path() must be non-blank and start with '/'.");
        }
        while (rawPath.length() > 1 && rawPath.endsWith("/")) {
            rawPath = rawPath.substring(0, rawPath.length() - 1);
        }

        String prefix = annotation.name().isBlank() ? field.getName() : annotation.name();
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String path = rawPath + "-" + sanitize(prefix) + "-" + suffix;

        LOG.debug("Registered MockServer path '{}'", path);
        return new HttpHandle(path, mockServer.getEndpoint(), sharedClient);
    }

    private static void ensureMockServer() {
        MockServerContainer local = mockServer;
        if (local != null && local.isRunning()) {
            return;
        }
        synchronized (LOCK) {
            if (mockServer != null && mockServer.isRunning()) {
                return;
            }
            MockServerContainer container = new MockServerContainer(IMAGE);
            // Rancher Desktop / cold pulls can exceed Testcontainers' default 60s.
            container.withStartupTimeout(Duration.ofMinutes(3));
            container.start();
            mockServer = container;
            sharedClient = new MockServerClient(container.getHost(), container.getServerPort());
            Runtime.getRuntime().addShutdownHook(
                new Thread(container::stop, "flink-testkit-mockserver-shutdown"));
            LOG.info("Shared MockServer started at {}", container.getEndpoint());
        }
    }

    private static String sanitize(String raw) {
        return raw.replaceAll("[^A-Za-z0-9_-]", "_");
    }
}
