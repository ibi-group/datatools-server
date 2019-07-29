package com.conveyal.datatools.manager.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;

/**
 * This represents the Auth0 API access token that is needed for requests to the v2 Management API. This token can be
 * retrieved by sending a request to the oauth token endpoint: https://auth0.com/docs/api/management/v2/get-access-tokens-for-production
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Auth0AccessToken {
    public String access_token;
    /** Seconds until token expires. */
    public int expires_in;
    public String scope;
    public String token_type;
    /**
     * Time when the object was instantiated. This is used in conjunction with expires_in to determine if a token is
     * still valid.
     */
    @JsonIgnore
    public long creation_time = new Date().getTime();

    /** Helper method to determine the time in milliseconds since epoch that this token expires. */
    @JsonIgnore
    public long getExpirationTime() {
        return this.expires_in * 1000 + this.creation_time;
    }
}
