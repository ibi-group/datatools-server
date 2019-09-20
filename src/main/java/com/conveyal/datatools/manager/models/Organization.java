package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

/**
 * An organization represents a group of users and projects (with contained feed sources). Currently, a user can only
 * belong to one organization (although a future aim may be to extend this) and a project can only belong to one
 * organization.
 *
 * Organizations are primarily intended to help organize multi-tenant instances where there needs to be separation
 * between administrative control over users and projects.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Organization extends Model implements Serializable {
    private static final long serialVersionUID = 1L;

    public String name;
    public String logoUrl;
    public boolean active;
    public UsageTier usageTier;
    public Set<Extension> extensions = new HashSet<>();
    public long subscriptionBeginDate;
    public long subscriptionEndDate;

    public Organization () {}

    @JsonProperty("projects")
    public Collection<Project> projects() {
        return Persistence.projects.getFiltered(eq("organizationId", id));
    }

    @JsonProperty("totalServiceSeconds")
    public long totalServiceSeconds() {
        return projects().stream()
                .map(Project::retrieveProjectFeedSources)
                .flatMap(Collection::stream)
                .filter(fs -> fs.latestValidation() != null)
                .map(fs -> fs.latestValidation().avgDailyRevenueTime)
                .mapToLong(Long::longValue)
                .sum();
    }

    /**
     * Created by landon on 1/30/17.
     */
    public enum Extension {
        GTFS_PLUS,
        DEPLOYMENT,
        VALIDATOR,
        ALERTS,
        SIGN_CONFIG
    }

    /**
     * Created by landon on 1/30/17.
     */
    public enum UsageTier {
        LOW,
        MEDIUM,
        HIGH
    }
}
