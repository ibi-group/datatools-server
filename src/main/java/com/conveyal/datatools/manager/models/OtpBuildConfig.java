package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Created by demory on 3/8/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OtpBuildConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public Boolean fetchElevationUS;
    // FIXME: elevation bucket causing NPE issue if missing values when deploying to OTP
    // public S3Bucket elevationBucket;

    public Boolean stationTransfers;

    public Double subwayAccessTime;

    /** Currently only supports no-configuration fares, e.g. New York or San Francisco */
    public String fares;

    public OtpBuildConfig() {}

    public static class S3Bucket implements Serializable {
        private static final long serialVersionUID = 1L;
        public String accessKey;
        public String secretKey;
        public String bucketName;
    }
}
