package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * Created by demory on 3/30/16.
 */

public class RtdCarrier {

    @JsonProperty
    String AgencyId;

    @JsonProperty
    String AgencyName;

    @JsonProperty
    String AgencyPhone;

    @JsonProperty
    String RttAgencyName;

    @JsonProperty
    String RttEnabled;

    @JsonProperty
    String AgencyShortName;

    @JsonProperty
    String AgencyPublicId;

    @JsonProperty
    String AddressLat;

    @JsonProperty
    String AddressLon;

    @JsonProperty
    String DefaultRouteType;

    @JsonProperty
    String CarrierStatus;

    @JsonProperty
    String AgencyAddress;

    @JsonProperty
    String AgencyEmail;

    @JsonProperty
    String AgencyUrl;

    @JsonProperty
    String AgencyFareUrl;

    @JsonProperty
    String EditedBy;

    @JsonProperty
    String EditedDate;

    public RtdCarrier() {
    }

    /**
     * Construct an RtdCarrier given the provided feed source.
     * @param source
     */
    public RtdCarrier(FeedSource source) {
        AgencyId = getValueForField(source, MtcFeedResource.AGENCY_ID);
        AgencyPhone = getValueForField(source, "AgencyPhone");
        AgencyName = getValueForField(source, "AgencyName");
        RttAgencyName = getValueForField(source, "RttAgencyName");
        RttEnabled = getValueForField(source, "RttEnabled");
        AgencyShortName = getValueForField(source, "AgencyShortName");
        AgencyPublicId = getValueForField(source, "AgencyPublicId");
        AddressLat = getValueForField(source, "AddressLat");
        AddressLon = getValueForField(source, "AddressLon");
        DefaultRouteType = getValueForField(source, "DefaultRouteType");
        CarrierStatus = getValueForField(source, "CarrierStatus");
        AgencyAddress = getValueForField(source, "AgencyAddress");
        AgencyEmail = getValueForField(source, "AgencyEmail");
        AgencyUrl = getValueForField(source, "AgencyUrl");
        AgencyFareUrl = getValueForField(source, "AgencyFareUrl");
    }

    private String getPropId(FeedSource source, String fieldName) {
        return constructId(source, MtcFeedResource.RESOURCE_TYPE, fieldName);
    }

    /**
     * FIXME: Are there cases where this might throw NPEs?
     */
    private String getValueForField (FeedSource source, String fieldName) {
        return Persistence.externalFeedSourceProperties.getById(getPropId(source, fieldName)).value;
    }
}