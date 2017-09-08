package com.conveyal.datatools.manager.models;

import com.conveyal.datatools.manager.persistence.DataStore;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by landon on 4/15/16.
 */
public class Region extends Model {
    private static final long serialVersionUID = 1L;

    private static DataStore<Region> regionStore = new DataStore<>("region");

    /** The name of this region, e.g. Atlanta. */
    public String name;

    // Polygon geometry of region as GeoJSON string
    @JsonIgnore
    public String geometry;
    public Double lat, lon;
    // hierarchical order of region: country, 1st order admin, or region
    public String order;

    public Boolean isPublic;
    public String defaultLanguage;
    public String defaultTimeZone;

    //@JsonView
    public Collection<FeedSource> feedSources;

    public Double north, south, east, west;

    public Region() {

    }

    /**
     * Get all of the FeedCollections that are defined
     */
    public static Collection<Region> retrieveAll() {
        return regionStore.getAll();
    }

    public static void deleteAll () {
        Region.retrieveAll().forEach(region -> region.delete());
    }

    public static Region retrieve(String id) {
        return regionStore.getById(id);
    }

    public void save() {
        save(true);
    }

    public void save(boolean commit) {
        if (commit)
            regionStore.save(this.id, this);
        else
            regionStore.saveWithoutCommit(this.id, this);
    }

    public void delete() {
//        for (FeedSource fs : getRegionFeedSources()) {
//            Arrays.asList(fs.regions).remove(this.id);
//            fs.save();
//        }

        regionStore.delete(this.id);
    }

    public static void commit () {
        regionStore.commit();
    }

    /**
     * Get all the feed sources for this feed collection
     */

//    @JsonIgnore
//    public Collection<? extends FeedSource> getRegionFeedSources() {
//
//        // TODO: use index, but not important for now because we generally only have one FeedCollection
////        if (this.id != null && fs.regions != null)
//        return Persistence.getFeedSources().stream().filter(fs -> Arrays.asList(fs.regions).contains(this.id)).collect(Collectors.toList());
//
//    }
}
