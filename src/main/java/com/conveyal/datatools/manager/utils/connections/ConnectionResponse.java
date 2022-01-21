package com.conveyal.datatools.manager.utils.connections;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface for getting HTTP connection response data.
 */
public interface ConnectionResponse {
    int getResponseCode() throws IOException;

    String getResponseMessage() throws IOException;

    String getRedirectUrl();

    InputStream getInputStream() throws IOException;

    Long getLastModified();
}
