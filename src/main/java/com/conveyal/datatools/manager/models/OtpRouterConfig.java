package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.Collection;

/**
 * Created by demory on 3/8/15.
 */

public class OtpRouterConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public Integer numItineraries;

    public Double  walkSpeed;

    public Double stairsReluctance;

    public Double carDropoffTime;

    public Collection<Updater> updaters;

    public static class Updater implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type;

        public Integer frequencySec;

        public String sourceType;

        public String url;

        public String defaultAgencyId;
    }

    public String brandingUrlRoot;

    public String requestLogFile;
}
