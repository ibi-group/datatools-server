package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.List;

/**
 * Created by landon on 5/20/16.
 */
public class OtpServer implements Serializable {
    private static final long serialVersionUID = 1L;
    public String name;
    public List<String> internalUrl;
    public String publicUrl;
    public Boolean admin;
    public String s3Bucket;
    public String s3Credentials;

    /**
     * Convert the name field into a string with no special characters.
     *
     * FIXME: This is currently used to keep track of which deployments have been deployed to which servers (it is used
     * for the {@link Deployment#deployedTo} field), but we should likely.
     */
    public String target() {
        return name != null ? name.replaceAll("[^a-zA-Z0-9]", "_") : null;
    }
}
