package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.List;

/**
 * Created by landon on 5/20/16.
 */
public class OtpServer implements Serializable {
    public String name;
    public List<String> internalUrl;
    public String publicUrl;
    public Boolean admin;
    public String s3Bucket;
    public String s3Credentials;
}
