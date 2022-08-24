package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.editor.utils.JacksonSerializers;
import com.conveyal.gtfs.validator.ValidationResult;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.Date;

/**
 * Includes summary data (a subset of fields) for a feed version.
 */
public class FeedVersionSummary extends Model implements Serializable {
    private static final long serialVersionUID = 1L;

    public FeedRetrievalMethod retrievalMethod;
    public int version;
    public String feedSourceId;
    public String name;
    public String namespace;
    public String originNamespace;
    public Long fileSize;
    public Date updated;
    /** Only a subset of the validation results are serialized to JSON via getValidationSummary. */
    @JsonIgnore
    public ValidationResult validationResult;
    private PartialValidationSummary validationSummary;

    public PartialValidationSummary getValidationSummary() {
        if (validationSummary == null) {
            validationSummary = new PartialValidationSummary();
        }
        return validationSummary;
    }

    /** Empty constructor for serialization */
    public FeedVersionSummary() {
        // Do nothing
    }

    /**
     * Holds a subset of fields from {@link:FeedValidationResultSummary} for UI use only.
     */
    public class PartialValidationSummary {
        /** Copied from FeedVersion */
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate startDate;

        /** Copied from FeedVersion */
        @JsonSerialize(using = JacksonSerializers.LocalDateIsoSerializer.class)
        @JsonDeserialize(using = JacksonSerializers.LocalDateIsoDeserializer.class)
        public LocalDate endDate;

        PartialValidationSummary() {
            // Older feeds created in datatools may not have validationResult
            if (validationResult != null) {
                this.startDate = validationResult.firstCalendarDate;
                this.endDate = validationResult.lastCalendarDate;
            }
        }
    }
}
