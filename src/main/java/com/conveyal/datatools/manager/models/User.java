package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

/**
 * Data about a user that can login and use a datatools application
 */
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class User extends Model implements Serializable {
    private static final long serialVersionUID = 1L;

    // If auth0 is enabled, this will always match the Auth0 user_id
    public String id;

    public String email;
}
