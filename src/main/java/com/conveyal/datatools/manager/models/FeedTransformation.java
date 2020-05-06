package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.common.status.MonitorableJob;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * This abstract class is the base for arbitrary feed transformations.
 *
 * Note: Subclasses do not need the {@link BsonDiscriminator} annotation. This is used by MongoDB to handle
 * polymorphism, but it only need be applied to the abstract parent class.
 */
@BsonDiscriminator
public abstract class FeedTransformation extends Model {
    public String csvData;
    public String sourceVersionId;
    public String table;

    public FeedTransformation() {}

    // TODO Should we add an isValid function that determines if the transformation is defined correctly? Maybe
    //  it could return something object that contains a bool + message.
    // boolean isValid();

    public abstract void transform(FeedVersion target, MonitorableJob.Status status);

    public abstract boolean isAppliedBeforeLoad();
}

