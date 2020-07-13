package com.conveyal.datatools.manager.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * An instance of this class represents all of the changes applied to the tables modified by a set of
 * {@link com.conveyal.datatools.manager.models.transform.FeedTransformation}.
 */
public class FeedTransformResult implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * A list of all of the transform results for each table name. Note: this is not a Map nor a Multimap because those
     * cannot be easily (de)serialized by MongoDB.
     */
    public List<TableTransformResult> tableTransformResults = new ArrayList<>();

    public FeedTransformResult() {}
}
