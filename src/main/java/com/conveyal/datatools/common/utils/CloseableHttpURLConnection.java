package com.conveyal.datatools.common.utils;

import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * A wrapper around HttpURLConnection that implements Closeable so it can be used in try-with-resources blocks.
 */
public class CloseableHttpURLConnection implements Closeable {
    private final HttpURLConnection connection;

    public CloseableHttpURLConnection(URL url) throws IOException {
        this.connection = (HttpURLConnection) url.openConnection();
    }

    public HttpURLConnection getConnection() {
        return connection;
    }

    @Override
    public void close() {
        if (connection != null) {
            connection.disconnect();
        }
    }
}
