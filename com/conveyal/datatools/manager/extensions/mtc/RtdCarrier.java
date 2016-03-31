package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.manager.models.FeedSource;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    /*public void mapFeedSource(FeedSource source){
        source.defaultGtfsId = this.AgencyId;
        source.shortName = this.AgencyShortName;
        source.AgencyPhone = this.AgencyPhone;
        source.RttAgencyName = this.RttAgencyName;
        source.RttEnabled = this.RttEnabled;
        source.AgencyShortName = this.AgencyShortName;
        source.AgencyPublicId = this.AgencyPublicId;
        source.AddressLat = this.AddressLat;
        source.AddressLon = this.AddressLon;
        source.DefaultRouteType = this.DefaultRouteType;
        source.CarrierStatus = this.CarrierStatus;
        source.AgencyAddress = this.AgencyAddress;
        source.AgencyEmail = this.AgencyEmail;
        source.AgencyUrl = this.AgencyUrl;
        source.AgencyFareUrl = this.AgencyFareUrl;

        source.save();
    }*/
}