package com.conveyal.datatools.manager.utils;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SimpleHttpResponse {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleHttpResponse.class);
    public final int status;
    public final String body;

    public SimpleHttpResponse(HttpResponse httpResponse) throws IOException {
        status = httpResponse.getStatusLine().getStatusCode();
        String bodyString = null;
        try {
            bodyString = EntityUtils.toString(httpResponse.getEntity());
        } catch (IOException e) {
            LOG.warn("Could not convert HTTP response to string", e);
            throw e;
        }
        body = bodyString;
    }
}
