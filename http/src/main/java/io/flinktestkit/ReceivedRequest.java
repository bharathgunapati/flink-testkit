package io.flinktestkit;

import java.util.List;
import java.util.Map;

/**
 * A single recorded HTTP request captured by MockServer.
 */
public final class ReceivedRequest {

    private final String method;
    private final String path;
    private final String body;
    private final Map<String, List<String>> headers;
    private final Map<String, List<String>> queryParams;

    ReceivedRequest(
            String method,
            String path,
            String body,
            Map<String, List<String>> headers,
            Map<String, List<String>> queryParams) {
        this.method = method;
        this.path = path;
        this.body = body;
        this.headers = headers;
        this.queryParams = queryParams;
    }

    public String method() {
        return method;
    }

    public String path() {
        return path;
    }

    public String body() {
        return body;
    }

    public Map<String, List<String>> headers() {
        return headers;
    }

    /** Query string parameters (empty when none were present). */
    public Map<String, List<String>> queryParams() {
        return queryParams;
    }

    @Override
    public String toString() {
        return "ReceivedRequest{method='" + method + "', path='" + path
            + "', queryParams=" + queryParams + ", body='" + body + "'}";
    }
}
