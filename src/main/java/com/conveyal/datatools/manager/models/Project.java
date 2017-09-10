package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    /** The name of this feed collection, e.g. NYSDOT. */
    public String name;

    public boolean useCustomOsmBounds;

    // TODO either use primitives or a reference to a "bounds" object containing primitives
    public Double osmNorth, osmSouth, osmEast, osmWest;

    public OtpBuildConfig buildConfig;

    public OtpRouterConfig routerConfig;

    public Collection<OtpServer> otpServers;

    public String organizationId;

    public OtpServer retrieveServer(String name) {
        for (OtpServer otpServer : otpServers) {
            if (otpServer.name.equals(name)) {
                return otpServer;
            }
        }
        return null;
    }

    public String defaultTimeZone;

    public String defaultLanguage;

    public transient Collection<FeedSource> feedSources;

    public double defaultLocationLat, defaultLocationLon;
    public boolean autoFetchFeeds;
    public int autoFetchHour, autoFetchMinute;

    // TODO either use primitives or a reference to a "bounds" object containing primitives
    public Double north, south, east, west;

    public Project() {
        this.buildConfig = new OtpBuildConfig();
        this.routerConfig = new OtpRouterConfig();
        this.useCustomOsmBounds = false;
    }

    /**
     * Get all of the FeedCollections that are defined
     */
    public static Collection<Project> retrieveAll() {
//        return projectStore.getAll();
        return null;
    }

    public static Project retrieve(String id) {
//        return projectStore.getById(id);
        return null;
    }

    public void save() {
        save(true);
    }

    public void save(boolean commit) {
//        if (commit)
//            projectStore.save(this.id, this);
//        else
//            projectStore.saveWithoutCommit(this.id, this);
    }


    public static void commit () {
//        projectStore.commit();
    }

    /**
     * Get all the feed sources for this feed collection
     */
    public Collection<FeedSource> retrieveProjectFeedSources() {
//        ArrayList<? extends FeedSource> ret = new ArrayList<>();

        // TODO: use index, but not important for now because we generally only have one FeedCollection
        return Persistence.feedSources.getAll().stream()
                .filter(fs -> this.id.equals(fs.projectId))
                .collect(Collectors.toList());

    }

    @JsonProperty("numberOfFeeds")
    public int numberOfFeeds() {
        return retrieveProjectFeedSources().size();
    }

    /**
     * Get all the deployments for this feed collection
     */
    public Collection<Deployment> retrieveDeployments() {
        ArrayList<Deployment> ret = Deployment.retrieveAll().stream()
                .filter(d -> this.id.equals(d.projectId))
                .collect(Collectors.toCollection(ArrayList::new));

        return ret;
    }

    public Organization retrieveOrganization() {
        if (organizationId != null) {
            return Organization.retrieve(organizationId);
        } else {
            return null;
        }
    }
}
