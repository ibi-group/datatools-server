package com.conveyal.datatools.manager.models;

import java.io.Serializable;

/**
 * Created by demory on 3/8/15.
 */

public class OtpBuildConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public Boolean fetchElevationUS;

    public Boolean stationTransfers;

    public Double subwayAccessTime;

    /** Currently only supports no-configuration fares, e.g. New York or San Francisco */
    public String fares;
}
