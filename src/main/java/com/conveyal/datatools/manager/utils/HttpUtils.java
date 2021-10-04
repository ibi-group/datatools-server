package com.conveyal.datatools.manager.utils;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HttpUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);
    public enum REQUEST_METHOD {GET, POST, DELETE, PUT}

    /**
     * Makes an http get/post/etc. request and returns the response. The request is based on the provided params.
     * @return a {@link SimpleHttpResponse} that consumes and closes the entity (verifying that the HTTP connection is
     *   closed)
     */
    public static SimpleHttpResponse httpRequestRawResponse(
            URI uri,
            int connectionTimeout,
            REQUEST_METHOD method,
            String bodyContent
    ) {
        return httpRequestRawResponse(uri, connectionTimeout, method, bodyContent, new ArrayList<>());
    }

    public static SimpleHttpResponse httpRequestRawResponse(
        URI uri,
        int connectionTimeout,
        REQUEST_METHOD method,
        String bodyContent,
        List<Header> headers
    ) {
        RequestConfig timeoutConfig = RequestConfig.custom()
            .setConnectionRequestTimeout(connectionTimeout)
            .setConnectTimeout(connectionTimeout)
            .setSocketTimeout(connectionTimeout)
            .build();

        HttpUriRequest httpUriRequest;

        // TODO: Add additional HTTP method types.
        switch (method) {
            case GET:
                HttpGet getRequest = new HttpGet(uri);
                for (Header header : headers) {
                    getRequest.setHeader(header);
                }
                getRequest.setConfig(timeoutConfig);
                httpUriRequest = getRequest;
                break;
            case POST:
                try {
                    HttpPost postRequest = new HttpPost(uri);
                    if (bodyContent != null) postRequest.setEntity(new StringEntity(bodyContent));
                    for (Header header : headers) {
                        postRequest.setHeader(header);
                    }

                    postRequest.setConfig(timeoutConfig);
                    httpUriRequest = postRequest;
                } catch (UnsupportedEncodingException e) {
                    LOG.error("Unsupported encoding type", e);
                    return null;
                }
                break;
            case PUT:
                try {
                    HttpPut putRequest = new HttpPut(uri);
                    for (Header header : headers) {
                        putRequest.setHeader(header);
                    }
                    putRequest.setEntity(new StringEntity(bodyContent));
                    putRequest.setConfig(timeoutConfig);
                    httpUriRequest = putRequest;
                } catch (UnsupportedEncodingException e) {
                    LOG.error("Unsupported encoding type", e);
                    return null;
                }
                break;
            case DELETE:
                HttpDelete deleteRequest = new HttpDelete(uri);
                for (Header header : headers) {
                    deleteRequest.setHeader(header);
                }
                deleteRequest.setConfig(timeoutConfig);
                httpUriRequest = deleteRequest;
                break;
            default:
                LOG.error("Request method type unknown: {}", method);
                return null;
        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(httpUriRequest)) {
            return new SimpleHttpResponse(response);
        } catch (IOException e) {
            LOG.error("An exception occurred while making a request to {}", uri, e);
            return null;
        }
    }
}
