package com.conveyal.datatools.manager.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collection;

/**
 * Created by demory on 3/8/15.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OtpRouterConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public Double driveDistanceReluctance;

    public Double stairsReluctance;

    public Collection<Updater> updaters;

    public ItineraryFilter itineraryFilters;

    public static class ItineraryFilter implements Serializable {
        private static final long serialVersionUID = 1L;
        public String nonTransitGeneralizedCostLimit;
    }

    public static class Updater implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type;

        public Integer frequencySec;

        public String sourceType;

        public String url;

        public String feedId;
    }
    
    public String requestLogFile;
}
