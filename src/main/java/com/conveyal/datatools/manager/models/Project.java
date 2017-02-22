package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.sun.org.apache.bcel.internal.classfile.Unknown;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a collection of feed sources that can be made into a deployment.
 * Generally, this would represent one agency that is managing the data.
 * For now, there is one FeedCollection per instance of GTFS data manager, but
 * we're trying to write the code in such a way that this is not necessary.
 *
 * @author mattwigway
 *
 */
@JsonInclude(Include.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Project extends Model {
    private static final long serialVersionUID = 1L;

    private static DataStore<Project> projectStore = new DataStore<>("projects");

    /** The name of this feed collection, e.g. NYSDOT. */
    public String name;

    public Boolean useCustomOsmBounds;

    public Double osmNorth, osmSouth, osmEast, osmWest;

    public OtpBuildConfig buildConfig;

    public OtpRouterConfig routerConfig;

    public Collection<OtpServer> otpServers;

    public String organizationId;

    @JsonIgnore
    public OtpServer getServer (String name) {
        for (OtpServer otpServer : otpServers) {
            if (otpServer.name.equals(name)) {
                return otpServer;
            }
        }
        return null;
    }

    public String defaultTimeZone;

    public String defaultLanguage;

    //@JsonView
    public Collection<FeedSource> feedSources;

    public Double defaultLocationLat, defaultLocationLon;
    public Boolean autoFetchFeeds;
    public int autoFetchHour, autoFetchMinute;

//    public Map<String, Double> boundingBox = new HashMap<>();

    public Double north, south, east, west;

    public Project() {
        this.buildConfig = new OtpBuildConfig();
        this.routerConfig = new OtpRouterConfig();
        this.useCustomOsmBounds = false;
    }

    /**
     * Get all of the FeedCollections that are defined
     */
    public static Collection<Project> getAll () {
        return projectStore.getAll();
    }

    public static Project get(String id) {
        return projectStore.getById(id);
    }

    public void save() {
        save(true);
    }

    public void save(boolean commit) {
        if (commit)
            projectStore.save(this.id, this);
        else
            projectStore.saveWithoutCommit(this.id, this);
    }

    public void delete() {
        for (FeedSource s : getProjectFeedSources()) {
            s.delete();
        }
        for (Deployment d : getProjectDeployments()) {
            d.delete();
        }

        projectStore.delete(this.id);
    }

    public static void commit () {
        projectStore.commit();
    }

    /**
     * Get all the feed sources for this feed collection
     */
    @JsonIgnore
    public Collection<FeedSource> getProjectFeedSources() {
//        ArrayList<? extends FeedSource> ret = new ArrayList<>();

        // TODO: use index, but not important for now because we generally only have one FeedCollection
        return FeedSource.getAll().stream().filter(fs -> this.id.equals(fs.projectId)).collect(Collectors.toList());

    }
    public int getNumberOfFeeds () {
        return FeedSource.getAll().stream().filter(fs -> this.id.equals(fs.projectId)).collect(Collectors.toList()).size();
    }
    /**
     * Get all the deployments for this feed collection
     */

    @JsonIgnore
    public Collection<Deployment> getProjectDeployments() {
        ArrayList<Deployment> ret = Deployment.getAll().stream()
                .filter(d -> this.id.equals(d.projectId))
                .collect(Collectors.toCollection(ArrayList::new));

        return ret;
    }

    @JsonIgnore
    public Organization getOrganization() {
        if (organizationId != null) {
            return Organization.get(organizationId);
        } else {
            return null;
        }
    }
}
