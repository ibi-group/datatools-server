package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by landon on 1/30/17.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Organization extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static DataStore<Organization> organizationStore = new DataStore<>("organizations");

    public String name;
    public String logoUrl;
    public boolean active;
    public UsageTier usageTier;
    public Set<Extension> extensions = new HashSet<>();
    public Date subscriptionBeginDate;
    public Date subscriptionEndDate;

    public Organization () {}

    public void save () {
        save(true);
    }

    public void save(boolean commit) {
        if (commit)
            organizationStore.save(id, this);
        else
            organizationStore.saveWithoutCommit(id, this);
    }

    public static Organization retrieve(String id) {
        return organizationStore.getById(id);
    }

    public static Collection<Organization> retrieveAll() {
        return organizationStore.getAll();
    }

    public static void commit() {
        organizationStore.commit();
    }

    public void delete() {
        organizationStore.delete(this.id);
    }

    @JsonProperty("projects")
    public Collection<Project> projects() {
        return Persistence.getProjects().stream().filter(p -> id.equals(p.organizationId)).collect(Collectors.toList());
    }

    @JsonProperty("totalServiceSeconds")
    public long totalServiceSeconds() {
        return projects().stream()
                .map(p -> p.retrieveProjectFeedSources())
                .flatMap(p -> p.stream())
                .filter(fs -> fs.latestValidation() != null)
                .map(fs -> fs.latestValidation().avgDailyRevenueTime)
                .mapToLong(Long::longValue)
                .sum();
    }

    /**
     * Created by landon on 1/30/17.
     */
    public static enum Extension {
        GTFS_PLUS,
        DEPLOYMENT,
        VALIDATOR,
        ALERTS,
        SIGN_CONFIG
    }

    /**
     * Created by landon on 1/30/17.
     */
    public static enum UsageTier {
        LOW,
        MEDIUM,
        HIGH
    }
}
