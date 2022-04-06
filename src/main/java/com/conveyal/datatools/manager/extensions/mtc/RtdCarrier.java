package com.conveyal.datatools.manager.extensions.mtc;

import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Field;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * Represents all of the properties persisted on a carrier record by the external MTC database known as RTD.
 *
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

    /** Empty constructor needed for serialization (also used to create empty carrier). */
    public RtdCarrier() {
    }

    /**
     * Construct an RtdCarrier given the provided feed source and initialize all field values from MongoDB.
     * @param source
     */
    public RtdCarrier(FeedSource source) {
        AgencyId = getValueForField(source, MtcFeedResource.AGENCY_ID_FIELDNAME);
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
     * Get the value stored in the database for a particular field.
     */
    private String getValueForField (FeedSource source, String fieldName) {
        ExternalFeedSourceProperty property = Persistence.externalFeedSourceProperties.getById(
            getPropId(source, fieldName)
        );
        return property != null ? property.value : null;
    }

    /**
     * Use reflection to update (or create if field does not exist) all fields for a carrier instance and provided feed
     * source.
     *
     * TODO: Perhaps we should not be using reflection, but it works pretty well here.
     */
    public void updateFields(FeedSource feedSource) throws IllegalAccessException {
        // Using reflection, iterate over every field in the class.
        for (Field carrierField : this.getClass().getDeclaredFields()) {
            String fieldName = carrierField.getName();
            String fieldValue = carrierField.get(this) != null ? carrierField.get(this).toString() : null;
            // Construct external feed source property for field with value from carrier.
            ExternalFeedSourceProperty prop = new ExternalFeedSourceProperty(
                feedSource,
                MtcFeedResource.RESOURCE_TYPE,
                fieldName,
                fieldValue
            );
            // If field does not exist, create it. Otherwise, update value.
            if (Persistence.externalFeedSourceProperties.getById(prop.id) == null) {
                Persistence.externalFeedSourceProperties.create(prop);
            } else {
                Persistence.externalFeedSourceProperties.updateField(prop.id, fieldName, fieldValue);
            }
        }
    }

    /**
     * Uses reflection to update the specified field/property of a carrier instance (see above).
     */
    public void updateProperty(ExternalFeedSourceProperty updatedProperty) {
        for (Field carrierField : this.getClass().getDeclaredFields()) {
            String fieldName = carrierField.getName();
            if (fieldName.equals(updatedProperty.name)) {
                try {
                    carrierField.set(this, updatedProperty.value);
                } catch (IllegalAccessException iae) {
                    // Should not be thrown.
                    iae.printStackTrace();
                }
                break;
            }
        }
    }

    /**
     * Converts to JSON for sending to the external publishing system (RTD).
     */
    public String toJson() throws JsonProcessingException {
        return new ObjectMapper()
            .writeValueAsString(this)
            // Per MTC, replace empty strings on the right-hand-side with null.
            .replaceAll(":\"\\s*\"", ":null");
    }
}