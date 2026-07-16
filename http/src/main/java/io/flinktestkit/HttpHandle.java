package io.flinktestkit;

import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.MediaType;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A typed handle onto a single, test-isolated HTTP path on the shared
 * MockServer.
 *
 * <p>Created and injected by {@link FlinkTestExtension} via the HTTP plugin —
 * test code should never construct this directly, only declare it via
 * {@link HttpEndpoint}.
 *
 * <p>Use {@link #stubPost}/{@link #stubGet} to seed responses,
 * {@link #awaitRequests} to wait for the job under test to call the endpoint
 * (recorded requests expose method, path, body, headers, and query params),
 * and {@link #url()}/{@link #baseUrl()} to configure the Flink HTTP connector.
 */
public final class HttpHandle {

    private final String path;
    private final String baseUrl;
    private final MockServerClient client;

    HttpHandle(String path, String baseUrl, MockServerClient client) {
        this.path = path;
        this.baseUrl = baseUrl;
        this.client = client;
    }

    /** Isolated path for this handle (includes a unique suffix). */
    public String path() {
        return path;
    }

    /** MockServer base URL, e.g. {@code http://localhost:32768}. */
    public String baseUrl() {
        return baseUrl;
    }

    /** Full URL for this endpoint ({@code baseUrl + path}). */
    public String url() {
        return baseUrl + path;
    }

    /** Stub {@code POST} requests to this path with the given status code. */
    public void stubPost(int statusCode) {
        stub("POST", statusCode, null);
    }

    /** Stub {@code GET} requests to this path with the given status code. */
    public void stubGet(int statusCode) {
        stub("GET", statusCode, null);
    }

    /** Stub {@code POST} with a JSON response body. */
    public void stubPostJson(int statusCode, String jsonBody) {
        stub("POST", statusCode, jsonBody);
    }

    /** Stub {@code GET} with a JSON response body. */
    public void stubGetJson(int statusCode, String jsonBody) {
        stub("GET", statusCode, jsonBody);
    }

    /** Stub any method with optional JSON body. */
    public void stub(String method, int statusCode, String jsonBody) {
        HttpResponse response = HttpResponse.response().withStatusCode(statusCode);
        if (jsonBody != null) {
            response = response
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(jsonBody);
        }
        client
            .when(HttpRequest.request().withMethod(method).withPath(path))
            .respond(response);
    }

    /**
     * Blocks until at least {@code expectedCount} requests hit this path,
     * then returns them.
     */
    public List<ReceivedRequest> awaitRequests(int expectedCount, Duration timeout) {
        if (expectedCount < 0) {
            throw new IllegalArgumentException("expectedCount must be >= 0");
        }
        return awaitRequests(reqs -> reqs.size() >= expectedCount, timeout);
    }

    /**
     * Blocks until recorded requests satisfy {@code condition}, polling on a
     * real completion condition instead of a fixed sleep.
     */
    public List<ReceivedRequest> awaitRequests(
            Predicate<List<ReceivedRequest>> condition, Duration timeout) {
        if (condition == null) {
            throw new IllegalArgumentException("condition is required");
        }
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }

        Instant deadline = Instant.now().plus(timeout);
        try {
            while (true) {
                List<ReceivedRequest> requests = recordedRequests();
                if (condition.test(requests)) {
                    return List.copyOf(requests);
                }
                if (Instant.now().isAfter(deadline)) {
                    throw new AssertionError(
                        "Timed out after " + timeout + " waiting for requests on path '"
                            + path + "'. Last seen " + requests.size() + " request(s): " + requests);
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while awaiting requests on '" + path + "'", e);
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed while awaiting requests on '" + path + "'", e);
        }
    }

    /** Returns every request recorded for this path so far. */
    public List<ReceivedRequest> recordedRequests() {
        HttpRequest[] recorded = client.retrieveRecordedRequests(
            HttpRequest.request().withPath(path));
        if (recorded == null || recorded.length == 0) {
            return List.of();
        }
        List<ReceivedRequest> result = new ArrayList<>(recorded.length);
        for (HttpRequest request : recorded) {
            result.add(toReceived(request));
        }
        return result;
    }

    void clear() {
        client.clear(HttpRequest.request().withPath(path));
    }

    private static ReceivedRequest toReceived(HttpRequest request) {
        String body = request.getBodyAsString();
        Map<String, List<String>> headers = new LinkedHashMap<>();
        if (request.getHeaderList() != null) {
            request.getHeaderList().forEach(header ->
                headers.put(
                    header.getName().getValue(),
                    header.getValues() == null
                        ? List.of()
                        : header.getValues().stream().map(v -> v.getValue()).toList()));
        }
        Map<String, List<String>> queryParams = new LinkedHashMap<>();
        if (request.getQueryStringParameterList() != null) {
            request.getQueryStringParameterList().forEach(param ->
                queryParams.put(
                    param.getName().getValue(),
                    param.getValues() == null
                        ? List.of()
                        : param.getValues().stream().map(v -> v.getValue()).toList()));
        }
        return new ReceivedRequest(
            request.getMethod() == null ? "" : request.getMethod().getValue(),
            request.getPath() == null ? "" : request.getPath().getValue(),
            body == null ? "" : body,
            Collections.unmodifiableMap(headers),
            Collections.unmodifiableMap(queryParams));
    }
}
