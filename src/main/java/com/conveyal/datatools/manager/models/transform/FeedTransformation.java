package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.utils.GtfsUtils;
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
    @JsonSubTypes.Type(value = ReplaceFileFromStringTransformation.class, name = "ReplaceFileFromStringTransformation"),
    @JsonSubTypes.Type(value = PreserveCustomFieldsTransformation.class, name = "PreserveCustomFieldsTransformation"),
    @JsonSubTypes.Type(value = AddCustomFileFromStringTransformation.class, name = "AddCustomFileTransformation")
})
public abstract class FeedTransformation<Target extends FeedTransformTarget> implements Serializable {
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

    public void doTransform(FeedTransformTarget target, MonitorableJob.Status status) {
        try {
            // Attempt to cast transform target to correct flavor
            // (fails the job if types mismatch.)
            Target typedTarget = (Target)target;

            // Validate parameters before running transform.
            validateTableName(status);
            validateFieldNames(status);
            // Let subclasses check parameters.
            validateParameters(status);
            if (status.error) {
                return;
            }

            // Pass the typed transform target to subclasses to transform.
            transform(typedTarget, status);
        } catch (ClassCastException classCastException) {
            status.fail(
                String.format("Transformation must be of type '%s'.", getTransformationTypeName())
            );
        } catch (Exception e) {
            status.fail(e.toString());
        }
    }

    protected abstract String getTransformationTypeName();

    /**
     * Contains the logic for this database-bound transformation.
     * @param target The database-bound or ZIP-file-bound target the transformation will operate on.
     * @param status Used to report success or failure status and details.
     */
    public abstract void transform(Target target, MonitorableJob.Status status) throws Exception;

    /**
     * At the moment, used by DbTransformation to validate field names.
     */
    protected abstract void validateFieldNames(MonitorableJob.Status status);

    /**
     * Handles validation logic prior to performing the transformation.
     * Calling status.fail prevents the transform logic from running.
     */
    public abstract void validateParameters(MonitorableJob.Status status);

    /**
     * Checks that the table name for the transform is valid.
     */
    protected void validateTableName(MonitorableJob.Status status) {
        // Validate fields before running transform.
        if (GtfsUtils.getGtfsTable(table) == null) {
            status.fail("Table must be valid GTFS spec table name (without .txt).");
        }
    }
}
