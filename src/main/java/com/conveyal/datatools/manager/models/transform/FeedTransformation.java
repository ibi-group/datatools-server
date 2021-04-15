package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.io.Serializable;

/**
 * This abstract class is the base for arbitrary feed transformations.
 *
 * Notes on Polymorphism:
 *
 * MongoDB: Subclasses do not need the {@link BsonDiscriminator} annotation. This is used by MongoDB to handle
 * polymorphism, but it only need be applied to the abstract parent class.
 *
 * Jackson (interaction with API requests): The {@link JsonTypeInfo} and {@link JsonSubTypes} annotations are used
 * in this class to indicate to Jackson how subtypes of FeedTransformation should be deserialized from API requests
 * There is more information on this approach here: https://stackoverflow.com/a/30386694/915811. In short, any new
 * FeedTransformation subtype must be explicitly registered with a JsonSubType entry. The name value (string literal)
 * must be present in a field called @type in the JSON representing the FeedTransformation.
 */
@BsonDiscriminator
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DeleteRecordsTransformation.class, name = "DeleteRecordsTransformation"),
    @JsonSubTypes.Type(value = NormalizeFieldTransformation.class, name = "NormalizeFieldTransformation"),
    @JsonSubTypes.Type(value = ReplaceFileFromVersionTransformation.class, name = "ReplaceFileFromVersionTransformation"),
    @JsonSubTypes.Type(value = ReplaceFileFromStringTransformation.class, name = "ReplaceFileFromStringTransformation")
})
public abstract class FeedTransformation implements Serializable {
    private static final long serialVersionUID = 1L;
    public String table;
    public boolean active = true;

    public FeedTransformation() {}

    public boolean isActive() {
        return active;
    }

    // TODO Should we add an isValid function that determines if the transformation is defined correctly? Maybe
    //  it could return something object that contains a bool + message.
    // boolean isValid();

    public abstract void doTransform(FeedTransformTarget target, MonitorableJob.Status status);

    /**
     * Overridable method that handles validation logic prior to performing the transformation.
     * Calling status.fail prevents the transform logic from running.
     * The default implementation does nothing.
     */
    public void validateParameters(MonitorableJob.Status status) { }
}
