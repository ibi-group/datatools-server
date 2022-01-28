package com.conveyal.datatools.manager.utils.connections;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

/**
 * Builds a {@link ConnectionResponse} instance sent to FeedSource from an {@link HttpURLConnection} instance.
 */
public class HttpURLConnectionResponse implements ConnectionResponse {
    private final HttpURLConnection connection;

    public HttpURLConnectionResponse(HttpURLConnection conn) {
        this.connection = conn;
    }

    public int getResponseCode() throws IOException {
        return connection.getResponseCode();
    }

    public InputStream getInputStream() throws IOException {
        return connection.getInputStream();
    }

    public String getResponseMessage() throws IOException {
        return connection.getResponseMessage();
    }

    public String getRedirectUrl() {
        return connection.getHeaderField("Location");
    }

    public Long getLastModified() {
        return connection.getLastModified();
    }
}
