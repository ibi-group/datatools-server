package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

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
public class Project extends Model {
    private static final long serialVersionUID = 1L;

    private static DataStore<Project> projectStore = new DataStore<>("projects");

    /** The name of this feed collection, e.g. NYSDOT. */
    public String name;

    //public Boolean useCustomOsmBounds;

    //public Double osmNorth, osmSouth, osmEast, osmWest;

    //public OtpBuildConfig buildConfig;

    //public OtpRouterConfig routerConfig;

    public String defaultTimeZone;

    public String defaultLanguage;

    public Double defaultLocationLat, defaultLocationLon;

    public Project() {
        /*this.buildConfig = new OtpBuildConfig();
        this.routerConfig = new OtpRouterConfig();
        this.useCustomOsmBounds = false;*/
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

    public static void commit () {
        projectStore.commit();
    }

    /**
     * Get all the feed sources for this feed collection
     */

    @JsonIgnore
    public Collection<FeedSource> getFeedSources () {
        ArrayList<FeedSource> ret = new ArrayList<FeedSource>();

        // TODO: use index, but not important for now because we generally only have one FeedCollection
        for (FeedSource fs : FeedSource.getAll()) {
            if (this.id.equals(fs.projectId)) {
                ret.add(fs);
            }
        }

        return ret;
    }

    /**
     * Get all the deployments for this feed collection
     */

    /*@JsonIgnore
    public Collection<Deployment> getDeployments () {
        ArrayList<Deployment> ret = new ArrayList<Deployment>();

        for (Deployment d : Deployment.getAll()) {
            if (this.id.equals(d.feedCollectionId)) {
                ret.add(d);
            }
        }

        return ret;
    }*/
}
