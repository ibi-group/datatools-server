package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonView;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * Created by landon on 1/30/17.
 */
public class Organization extends Model implements Serializable {
    private static final long serialVersionUID = 1L;
    private static DataStore<Organization> organizationStore = new DataStore<>("organizations");

    public String name;
    public String logoUrl;
    public boolean active;
    public UsageTier usageTier;
    public Extension[] extensions;
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

    public static Organization get (String id) {
        return organizationStore.getById(id);
    }

    public static Collection<Organization> getAll() {
        return organizationStore.getAll();
    }

    public static void commit() {
        organizationStore.commit();
    }

    public void delete() {
        organizationStore.delete(this.id);
    }

    public Collection<Project> getProjects() {
        return Project.getAll().stream().filter(p -> id.equals(p.organizationId)).collect(Collectors.toList());
    }

    public long getTotalServiceSeconds () {
        return Project.getAll().stream()
                .map(p -> p.getProjectFeedSources())
                .flatMap(p -> p.stream())
                .map(fs -> fs.getLatestValidation().avgDailyRevenueTime)
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
