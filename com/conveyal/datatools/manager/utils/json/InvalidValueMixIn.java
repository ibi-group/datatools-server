package com.conveyal.datatools.manager.utils.json;

import com.conveyal.gtfs.model.Priority;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Specify the annotations needed to construct an InvalidValue. This is a Jackson mixin so we don't need to
 * add a default constructor, etc.
 */
public abstract class InvalidValueMixIn {
    InvalidValueMixIn (
            @JsonProperty("affectedEntity") String affectedEntity,
            @JsonProperty("affectedField") String affectedField,
            @JsonProperty("affectedEntityId") String affectedEntityId,
            @JsonProperty("problemType") String problemType,
            @JsonProperty("problemDescription") String problemDescription,
            @JsonProperty("problemData") Object problemData,
            @JsonProperty("priority") Priority priority
    ) {};
}
