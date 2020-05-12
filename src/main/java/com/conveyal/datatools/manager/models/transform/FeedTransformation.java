package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.common.status.MonitorableJob;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.io.Serializable;

/**
 * This abstract class is the base for arbitrary feed transformations.
 *
 * Note: Subclasses do not need the {@link BsonDiscriminator} annotation. This is used by MongoDB to handle
 * polymorphism, but it only need be applied to the abstract parent class.
 */
@BsonDiscriminator
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

    public abstract void transform(FeedTransformTarget target, MonitorableJob.Status status);
}

