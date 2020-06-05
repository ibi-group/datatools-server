package com.conveyal.datatools.manager.models.transform;

import com.conveyal.datatools.manager.models.FeedRetrievalMethod;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class contains a set of transformations to apply to new versions for a feed source and a set of rules about how
 * those transformations apply. These rules include whether the transformations are active, which retrieval methods they
 * apply to (which should be mutually exclusive with other {@link FeedTransformRules} in {@link FeedSource#transformRules},
 * and whether a new version should result from the transformations.
 */
public class FeedTransformRules implements Serializable {
    private static final long serialVersionUID = 1L;
    /** Whether the rule set is active */
    public boolean active = true;
    /** Version retrieval methods to which this transform rule set applies. */
    public List<FeedRetrievalMethod> retrievalMethods = new ArrayList<>();
    /** List of transformations to be applied to feed in sequence */
    public List<FeedTransformation> transformations = new ArrayList<>();
    /**
     * Whether to create a new version after applying the (db) transformations (default behavior is simply to create a new
     * snapshot).
     */
    public boolean createNewVersion;

    /** no-arg constructor for serialization */
    public FeedTransformRules() {}

    /** Default constructor to create rule set with the passed in transformations */
    public FeedTransformRules(FeedTransformation transformation) {
        // Default retrieval methods to apply to are fetch/manual upload. In other words, transformations attached to this
        // rule set will only be applied to new versions that are fetched by URL or uploaded manually (not feeds
        // produced by the GTFS editor).
        this(transformation, FeedRetrievalMethod.FETCHED_AUTOMATICALLY, FeedRetrievalMethod.MANUALLY_UPLOADED);
    }

    public FeedTransformRules(FeedTransformation transformation, FeedRetrievalMethod ...retrievalMethods) {
        // Default retrieval methods to apply to are fetch/manual upload. In other words, transformations attached to this
        // rule set will only be applied to new versions that are fetched by URL or uploaded manually (not feeds
        // produced by the GTFS editor).
        this.retrievalMethods.addAll(Arrays.asList(retrievalMethods));
        this.transformations.add(transformation);
    }

    public boolean isActive() {
        return active;
    }

    public boolean hasRetrievalMethod(FeedRetrievalMethod retrievalMethod) {
        return retrievalMethods.contains(retrievalMethod);
    }

    public boolean shouldTransform(FeedVersion target) {
        return hasRetrievalMethod(target.retrievalMethod);
    }

    public <T extends FeedTransformation> boolean hasTransformationsOfType(FeedVersion target, Class<T> clazz) {
        return getActiveTransformations(target, clazz).size() > 0;
    }

    public <T extends FeedTransformation> List<T> getActiveTransformations(FeedVersion target, Class<T> clazz) {
        // First check whether target should be transformed (based on its retrieval method).
        if (shouldTransform(target) && this.isActive()) {
            return transformations.stream()
                // Filter out paused transformations.
                .filter(FeedTransformation::isActive)
                // Filter on and cast to provided type.
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }
}
